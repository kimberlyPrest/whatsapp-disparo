"""
WhatsApp Bulk Sender — FastAPI Backend (Multi-tenant + Supabase)
"""

import asyncio
import csv
import io
import json
import os
import random
from datetime import datetime
from typing import AsyncGenerator, Optional

import gspread
import requests
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

# ================================================================
# SUPABASE CONFIG
# ================================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://aucdgkmujqzhginokegx.supabase.co")
SUPABASE_SERVICE_KEY = os.getenv(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF1Y2Rna211anF6aGdpbm9rZWd4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzMyMzgwOCwiZXhwIjoyMDg4ODk5ODB9.VlbAvc2o2oOGsUTt7bQkkW93P8mQDHWJVdbveUfIA9s",
)
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF1Y2Rna211anF6aGdpbm9rZWd4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMzMjM4MDgsImV4cCI6MjA4ODg5OTgwOH0.s5TJJadX45oxRj6_HZu-bKbdAsn_1QDo_YOy5kV0-ao"

COL_NAME = "Nome"
COL_PHONE = "Telefone"

# ================================================================
# SUPABASE REST HELPERS (direct HTTP — avoids supabase-py header bugs)
# ================================================================

def _db_headers(prefer: str = "return=representation") -> dict:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _db_get(table: str, filters: dict = None, columns: str = "*", order: str = None, raw_params: dict = None) -> list:
    """
    filters: {col: val} → col=eq.val
    raw_params: passthrough params para filtros avançados (in, gte, ilike, etc.)
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params: dict = {"select": columns}
    if filters:
        for k, v in filters.items():
            params[k] = f"eq.{v}"
    if raw_params:
        params.update(raw_params)
    if order:
        params["order"] = order
    resp = requests.get(url, headers=_db_headers(), params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _db_upsert(table: str, data, on_conflict: str) -> list:
    """Upsert one or many rows. Returns list of upserted rows."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = _db_headers(prefer=f"resolution=merge-duplicates,return=representation")
    params = {"on_conflict": on_conflict}
    resp = requests.post(url, headers=headers, json=data, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _db_delete(table: str, filters: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = requests.delete(url, headers=_db_headers(prefer="return=minimal"), params=params, timeout=15)
    resp.raise_for_status()


def _db_insert(table: str, data) -> list:
    """Insert one row (dict) or many rows (list of dicts). Returns list of inserted rows."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.post(url, headers=_db_headers(), json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _db_patch(table: str, data: dict, filters: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = requests.patch(
        url, headers=_db_headers(prefer="return=minimal"), json=data, params=params, timeout=15
    )
    resp.raise_for_status()


# ================================================================
# APP
# ================================================================
app = FastAPI(title="WhatsApp Bulk Sender")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

security = HTTPBearer(auto_error=False)

# Per-user dispatch state: { user_id: { running, events, subscribers } }
_dispatches: dict[str, dict] = {}

# Shared HubSpot webhook contacts buffer (backward compat with existing dispatch)
_webhook_contacts: list = []

# Raw payloads received from HubSpot (last 50) — for inspection/schema discovery
_webhook_raw: list = []

# ================================================================
# SCHEDULER (agendamento de disparos)
# ================================================================
scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")


@app.on_event("startup")
async def startup_event():
    scheduler.add_job(check_scheduled_dispatches, "interval", minutes=1, id="check_scheduled")
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()


# ================================================================
# CSAT URLs por produto (Feature 5)
# ================================================================
CSAT_URLS = {
    "ELITE": "https://tally.so/r/wdg6KD?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
    "LABS":  "https://tally.so/r/nre1xl?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
    "SCALE": "https://form.adapta.org/r/9qqReQ?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
}


# ================================================================
# AUTH
# ================================================================
def _validate_token_sync(jwt_token: str) -> dict:
    """Validates user JWT by calling Supabase Auth REST API directly."""
    resp = requests.get(
        f"{SUPABASE_URL}/auth/v1/user",
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "apikey": SUPABASE_ANON_KEY,
        },
        timeout=10,
    )
    if resp.status_code != 200:
        raise ValueError(f"Token inválido (status {resp.status_code})")
    return resp.json()


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    token: Optional[str] = Query(default=None),
) -> str:
    jwt_token = None
    if credentials:
        jwt_token = credentials.credentials
    elif token:
        jwt_token = token

    if not jwt_token:
        raise HTTPException(status_code=401, detail="Token não fornecido.")

    try:
        loop = asyncio.get_event_loop()
        user_data = await loop.run_in_executor(None, _validate_token_sync, jwt_token)
        email = user_data.get("email", "")
        user_id = user_data.get("id", "")
        if not email.endswith("@adapta.org"):
            raise HTTPException(status_code=403, detail="Acesso restrito a emails @adapta.org.")
        return user_id
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Autenticação falhou: {str(e)}")


# ================================================================
# MODELS
# ================================================================
class DispatchRequest(BaseModel):
    source_type: str = "sheets"
    sheet_url: Optional[str] = None
    contacts_json: Optional[str] = None
    source_filename: Optional[str] = None
    template: str
    lot_every: int = 150
    lot_min: float = 500.0
    lot_max: float = 600.0
    sub_every: int = 30
    sub_min: float = 158.0
    sub_max: float = 200.0
    msg_min: float = 25.0
    msg_max: float = 50.0


class CredentialsRequest(BaseModel):
    evolution_api_url: str
    evolution_api_key: str
    instance_name: str
    google_credentials: Optional[dict] = None


class OwnerMappingRequest(BaseModel):
    hubspot_owner_name: str
    adapta_email: str


class ScheduleDispatchRequest(BaseModel):
    contacts_json: list
    template: str
    scheduled_at: str          # ISO 8601 datetime
    dispatch_type: str = "bulk"
    lot_every: int = 150
    lot_min: float = 500.0
    lot_max: float = 600.0
    sub_every: int = 30
    sub_min: float = 158.0
    sub_max: float = 200.0
    msg_min: float = 25.0
    msg_max: float = 50.0


class PostCallDispatchRequest(BaseModel):
    contact_ids: list[str]
    template: str = "Olá {primeiro_nome}! Obrigado pela reunião. Por favor, responda nossa pesquisa: {link_csat}"
    scheduled_at: Optional[str] = None  # None = enviar agora


# ================================================================
# HELPERS
# ================================================================
def normalize_phone(phone: str) -> str:
    digits = "".join(filter(str.isdigit, str(phone)))
    if not digits.startswith("55"):
        digits = "55" + digits
    return digits


def _parse_contacts(records: list[dict]) -> list[dict]:
    contacts = []
    for row in records:
        name = str(row.get(COL_NAME, "")).strip()
        phone = str(row.get(COL_PHONE, "")).strip()
        if name and phone:
            contacts.append(
                {
                    "name": name,
                    "first_name": name.split()[0].capitalize(),
                    "phone": normalize_phone(phone),
                }
            )
    return contacts


def load_from_sheet(sheet_url: str, google_creds: dict) -> list[dict]:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
    client = gspread.authorize(creds)
    raw = client.open_by_url(sheet_url).sheet1.get_all_records()
    records = [{k.strip(): v for k, v in row.items()} for row in raw]
    return _parse_contacts(records)


def parse_csv_contacts(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    records = [{k.strip(): v for k, v in row.items()} for row in reader]
    return _parse_contacts(records)


def render_msg(template: str, contact: dict) -> str:
    return template.replace("{primeiro_nome}", contact["first_name"])


def _send_sync(phone: str, message: str, api_url: str, api_key: str, instance: str) -> dict:
    url = f"{api_url}/message/sendText/{instance}"
    headers = {"apikey": api_key, "Content-Type": "application/json"}
    resp = requests.post(
        url, json={"number": phone, "text": message}, headers=headers, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def get_pause(index: int, cfg: dict) -> tuple[float, str]:
    if index % cfg["lot_every"] == 0:
        return random.uniform(cfg["lot_min"], cfg["lot_max"]), f"Pausa de lote — {index} msgs enviadas"
    if index % cfg["sub_every"] == 0:
        return random.uniform(cfg["sub_min"], cfg["sub_max"]), f"Pausa de sublote — {index} msgs enviadas"
    return random.uniform(cfg["msg_min"], cfg["msg_max"]), "Pausa individual"


def get_user_state(user_id: str) -> dict:
    if user_id not in _dispatches:
        _dispatches[user_id] = {"running": False, "events": [], "subscribers": []}
    return _dispatches[user_id]


async def _broadcast(user_id: str, event: dict) -> None:
    state = get_user_state(user_id)
    state["events"].append(event)
    for q in list(state["subscribers"]):
        await q.put(event)


def _parse_hubspot_payload(payload) -> list[dict]:
    """
    Parse HubSpot webhook payload into contacts with full properties.
    Handles both single object and list. Supports:
      - Simple: {"nome": "...", "telefone": "..."}
      - HubSpot native: {"properties": {"firstname": {"value":"..."}, ...}}
      - HubSpot flat:   {"properties": {"firstname": "...", ...}}

    NOTA (Fase 0): Os field names abaixo são estimativas baseadas na API do HubSpot.
    Após inspecionar /api/webhook/hubspot/raw com um payload real, ajuste os nomes
    nas variáveis de campo marcadas com # FIELD_NAME.
    """
    items = payload if isinstance(payload, list) else [payload]
    contacts = []

    for item in items:
        if not isinstance(item, dict):
            continue

        props = item.get("properties", {})

        def _prop(key):
            """Extrai valor de props independente do formato (flat ou nested)."""
            v = props.get(key, "") if props else ""
            if isinstance(v, dict):
                return str(v.get("value", "")).strip()
            return str(v or "").strip()

        # — Nome —
        name = str(item.get("nome") or item.get("name") or "").strip()
        if not name:
            firstname = _prop("firstname")                      # FIELD_NAME
            lastname = _prop("lastname")                        # FIELD_NAME
            name = f"{firstname} {lastname}".strip()

        # — Telefone —
        phone = str(item.get("telefone") or item.get("phone") or "").strip()
        if not phone:
            phone = (
                _prop("mobilephone")                            # FIELD_NAME
                or _prop("phone")                               # FIELD_NAME
                or _prop("telefone")                            # FIELD_NAME
                or _prop("whatsapp")                            # FIELD_NAME
            )

        name = name.strip()
        phone = phone.strip()

        if not name or not phone:
            continue

        # — Campos novos —
        email = _prop("email")                                  # FIELD_NAME
        etapa_negocio = _prop("dealstage") or _prop("etapa_negocio")    # FIELD_NAME
        produto = _prop("produto") or _prop("product_line")     # FIELD_NAME
        numero_consultoria = _prop("numero_consultoria") or _prop("hs_object_id")  # FIELD_NAME
        owner_name = _prop("hubspot_owner_name") or _prop("owner_name")            # FIELD_NAME

        # — Datas de reunião —
        data_reuniao_1 = _prop("data_reuniao_1") or _prop("hs_meeting_1_date") or None  # FIELD_NAME
        data_reuniao_2 = _prop("data_reuniao_2") or _prop("hs_meeting_2_date") or None  # FIELD_NAME
        data_reuniao_3 = _prop("data_reuniao_3") or _prop("hs_meeting_3_date") or None  # FIELD_NAME

        # — CSATs —
        csat_reuniao_1 = _prop("csat_reuniao_1") or None        # FIELD_NAME
        csat_reuniao_2 = _prop("csat_reuniao_2") or None        # FIELD_NAME
        csat_reuniao_3 = _prop("csat_reuniao_3") or None        # FIELD_NAME

        # — Data de entrada na etapa atual —
        data_etapa_atual = _prop("hs_stage_probabilities_start_date") or _prop("data_etapa_atual") or None  # FIELD_NAME

        # — HubSpot ID do deal/contato —
        hubspot_id = (
            str(item.get("id") or item.get("hs_object_id") or "").strip()
            or _prop("hs_object_id")
        ) or None

        contacts.append({
            # campos para disparo (compatibilidade com engine existente)
            "name": name,
            "first_name": name.split()[0].capitalize(),
            "phone": normalize_phone(phone),
            # campos novos
            "email": email or None,
            "etapa_negocio": etapa_negocio or None,
            "produto": produto or None,
            "numero_consultoria": numero_consultoria or None,
            "owner_name": owner_name or None,
            "data_reuniao_1": data_reuniao_1 or None,
            "data_reuniao_2": data_reuniao_2 or None,
            "data_reuniao_3": data_reuniao_3 or None,
            "csat_reuniao_1": csat_reuniao_1,
            "csat_reuniao_2": csat_reuniao_2,
            "csat_reuniao_3": csat_reuniao_3,
            "data_etapa_atual": data_etapa_atual or None,
            "hubspot_id": hubspot_id,
            "raw_payload": item,
        })

    return contacts


def _resolve_owner_user_id(owner_name: str) -> Optional[str]:
    """Retorna o adapta_user_id para o nome do proprietário HubSpot, ou None."""
    if not owner_name:
        return None
    try:
        rows = _db_get(
            "owner_mapping",
            raw_params={"hubspot_owner_name": f"ilike.{owner_name}"},
            columns="adapta_user_id,adapta_email",
        )
        if not rows:
            return None
        row = rows[0]
        # Se user_id ainda não foi resolvido, busca pelo email e salva
        if not row.get("adapta_user_id") and row.get("adapta_email"):
            auth_rows = _db_get(
                "users",
                raw_params={"email": f"eq.{row['adapta_email']}"},
                columns="id",
            )
            if auth_rows:
                uid = auth_rows[0]["id"]
                _db_patch("owner_mapping", {"adapta_user_id": uid},
                          {"adapta_email": row["adapta_email"]})
                return uid
        return row.get("adapta_user_id")
    except Exception:
        return None


def _upsert_hubspot_contact(contact: dict) -> None:
    """Persiste/atualiza contato HubSpot no banco. Silencia erros para não quebrar o webhook."""
    if not contact.get("hubspot_id"):
        return
    try:
        user_id = _resolve_owner_user_id(contact.get("owner_name"))
        row = {
            "hubspot_id": contact["hubspot_id"],
            "user_id": user_id,
            "nome": contact["name"],
            "primeiro_nome": contact["first_name"],
            "telefone": contact["phone"],
            "email": contact.get("email"),
            "etapa_negocio": contact.get("etapa_negocio"),
            "produto": contact.get("produto"),
            "numero_consultoria": contact.get("numero_consultoria"),
            "data_reuniao_1": contact.get("data_reuniao_1"),
            "data_reuniao_2": contact.get("data_reuniao_2"),
            "data_reuniao_3": contact.get("data_reuniao_3"),
            "csat_reuniao_1": contact.get("csat_reuniao_1"),
            "csat_reuniao_2": contact.get("csat_reuniao_2"),
            "csat_reuniao_3": contact.get("csat_reuniao_3"),
            "data_etapa_atual": contact.get("data_etapa_atual"),
            "raw_payload": contact.get("raw_payload"),
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
        _db_upsert("hubspot_contacts", row, on_conflict="hubspot_id")
    except Exception:
        pass


# ================================================================
# DISPATCH TASK
# ================================================================
async def run_dispatch(
    user_id: str,
    dispatch_id: str,
    contacts: list[dict],
    template: str,
    cfg: dict,
    api_url: str,
    api_key: str,
    instance: str,
) -> None:
    total = len(contacts)
    ok = fail = 0
    loop = asyncio.get_event_loop()
    logs_batch = []

    for i, contact in enumerate(contacts, 1):
        msg = render_msg(template, contact)
        try:
            await loop.run_in_executor(
                None, _send_sync, contact["phone"], msg, api_url, api_key, instance
            )
            status, err = "success", ""
            ok += 1
        except Exception as e:
            status = "error"
            err = str(e)[:120]
            fail += 1

        logs_batch.append(
            {
                "dispatch_id": dispatch_id,
                "contact_index": i,
                "contact_name": contact["name"],
                "contact_phone": contact["phone"],
                "status": status,
                "error_message": err or None,
            }
        )

        if len(logs_batch) >= 50:
            try:
                _db_insert("dispatch_logs", logs_batch)
            except Exception:
                pass
            logs_batch = []

        await _broadcast(
            user_id,
            {
                "type": "sent",
                "index": i,
                "total": total,
                "name": contact["name"],
                "phone": contact["phone"],
                "first": contact["first_name"],
                "status": status,
                "error": err,
                "ok": ok,
                "fail": fail,
            },
        )

        if i < total:
            pause_secs, pause_reason = get_pause(i, cfg)
            for remaining in range(int(pause_secs), 0, -1):
                await _broadcast(
                    user_id,
                    {
                        "type": "pause",
                        "seconds": remaining,
                        "total_pause": int(pause_secs),
                        "reason": pause_reason,
                    },
                )
                await asyncio.sleep(1)

    if logs_batch:
        try:
            _db_insert("dispatch_logs", logs_batch)
        except Exception:
            pass

    try:
        _db_patch(
            "dispatches",
            {
                "status": "completed",
                "sent_count": ok,
                "error_count": fail,
                "finished_at": datetime.utcnow().isoformat() + "Z",
            },
            {"id": dispatch_id},
        )
    except Exception:
        pass

    state = get_user_state(user_id)
    state["running"] = False
    await _broadcast(user_id, {"type": "done", "ok": ok, "fail": fail})


# ================================================================
# ROUTES
# ================================================================
@app.get("/")
def index():
    return FileResponse("index.html")


# ----------------------------------------------------------------
# HUBSPOT WEBHOOK — public endpoint (no auth required, single shared pool)
# URL: POST /api/webhook/hubspot
# ----------------------------------------------------------------
@app.post("/api/webhook/hubspot")
async def hubspot_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Payload JSON inválido.")

    # Store raw payload for schema inspection (keep last 50)
    items = body if isinstance(body, list) else [body]
    _webhook_raw.extend(items)
    if len(_webhook_raw) > 50:
        del _webhook_raw[:-50]

    new_contacts = _parse_hubspot_payload(body)

    # Backward compat: mantém buffer em memória para disparo direto
    _webhook_contacts.extend(new_contacts)

    # Fase 2: persiste/atualiza no banco (operação aditiva, não bloqueia resposta)
    loop = asyncio.get_event_loop()
    for contact in new_contacts:
        loop.run_in_executor(None, _upsert_hubspot_contact, contact)

    return {"received": True, "parsed": len(new_contacts)}


@app.get("/api/webhook-contacts")
def get_webhook_contacts(_: str = Depends(get_current_user)):
    return {"contacts": list(_webhook_contacts), "count": len(_webhook_contacts)}


@app.delete("/api/webhook-contacts")
def clear_webhook_contacts(_: str = Depends(get_current_user)):
    _webhook_contacts.clear()
    return {"status": "cleared"}


@app.get("/api/webhook/hubspot/raw")
def get_webhook_raw(_: str = Depends(get_current_user)):
    """Returns raw HubSpot payloads for schema inspection."""
    all_keys: dict = {}
    for item in _webhook_raw:
        for k, v in item.items():
            if k not in all_keys:
                all_keys[k] = type(v).__name__
    return {
        "count": len(_webhook_raw),
        "all_keys_seen": all_keys,
        "samples": _webhook_raw[-3:],  # last 3 payloads
    }


# ----------------------------------------------------------------
# CREDENTIALS
# ----------------------------------------------------------------
@app.get("/api/credentials")
def get_credentials(user_id: str = Depends(get_current_user)):
    try:
        rows = _db_get("user_credentials", filters={"user_id": user_id})
        if rows:
            cred = dict(rows[0])
            cred["has_google_credentials"] = bool(cred.get("google_credentials"))
            cred.pop("google_credentials", None)
            return cred
        return {}
    except Exception as e:
        raise HTTPException(500, f"Erro ao buscar credenciais: {str(e)}")


@app.post("/api/credentials")
def save_credentials(req: CredentialsRequest, user_id: str = Depends(get_current_user)):
    try:
        data = {
            "user_id": user_id,
            "evolution_api_url": req.evolution_api_url,
            "evolution_api_key": req.evolution_api_key,
            "instance_name": req.instance_name,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
        if req.google_credentials is not None:
            data["google_credentials"] = req.google_credentials

        existing = _db_get("user_credentials", filters={"user_id": user_id}, columns="id")
        if existing:
            _db_patch("user_credentials", data, {"user_id": user_id})
        else:
            data["created_at"] = datetime.utcnow().isoformat() + "Z"
            _db_insert("user_credentials", data)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar credenciais: {str(e)}")


# ----------------------------------------------------------------
# CONTACTS
# ----------------------------------------------------------------
@app.post("/api/load")
def load_contacts_route(body: dict, user_id: str = Depends(get_current_user)):
    sheet_url = body.get("sheet_url", "")
    if not sheet_url:
        raise HTTPException(400, "sheet_url obrigatório.")
    rows = _db_get("user_credentials", filters={"user_id": user_id}, columns="google_credentials")
    if not rows or not rows[0].get("google_credentials"):
        raise HTTPException(400, "Configure as credenciais Google primeiro em Configuração.")
    google_creds = rows[0]["google_credentials"]
    try:
        contacts = load_from_sheet(sheet_url, google_creds)
        return {"contacts": contacts}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...), _: str = Depends(get_current_user)):
    content = await file.read()
    try:
        contacts = parse_csv_contacts(content)
        return {"contacts": contacts, "filename": file.filename}
    except Exception as e:
        raise HTTPException(400, f"Erro ao processar CSV: {e}")


# ----------------------------------------------------------------
# DISPATCH
# ----------------------------------------------------------------
@app.post("/api/dispatch/start")
async def start_dispatch(
    req: DispatchRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    state = get_user_state(user_id)
    if state["running"]:
        raise HTTPException(409, "Disparo já em andamento.")

    rows = _db_get("user_credentials", filters={"user_id": user_id})
    if not rows:
        raise HTTPException(400, "Configure as credenciais primeiro em Configuração.")
    cred = rows[0]
    api_url = cred.get("evolution_api_url", "")
    api_key = cred.get("evolution_api_key", "")
    instance = cred.get("instance_name", "")
    google_creds = cred.get("google_credentials")

    if not all([api_url, api_key, instance]):
        raise HTTPException(400, "Credenciais Evolution API incompletas.")

    if req.source_type == "sheets":
        if not req.sheet_url:
            raise HTTPException(400, "URL da planilha obrigatória.")
        if not google_creds:
            raise HTTPException(400, "Configure as credenciais Google primeiro.")
        contacts = load_from_sheet(req.sheet_url, google_creds)
    elif req.source_type == "csv" and req.contacts_json:
        contacts = json.loads(req.contacts_json)
    elif req.source_type == "hubspot":
        contacts = list(_webhook_contacts)
        if not contacts:
            raise HTTPException(400, "Nenhum contato recebido via webhook HubSpot.")
    elif req.source_type == "clients" and req.contacts_json:
        contacts = json.loads(req.contacts_json)
    else:
        raise HTTPException(400, "Fonte de contatos inválida.")

    if not contacts:
        raise HTTPException(400, "Nenhum contato encontrado.")

    cfg = {
        "lot_every": req.lot_every, "lot_min": req.lot_min, "lot_max": req.lot_max,
        "sub_every": req.sub_every, "sub_min": req.sub_min, "sub_max": req.sub_max,
        "msg_min": req.msg_min, "msg_max": req.msg_max,
    }

    dispatch_data = {
        "user_id": user_id,
        "source_type": req.source_type,
        "source_url": req.sheet_url,
        "source_filename": req.source_filename,
        "template": req.template,
        "status": "running",
        "total_contacts": len(contacts),
        "lot_config": cfg,
        "started_at": datetime.utcnow().isoformat() + "Z",
    }
    inserted = _db_insert("dispatches", dispatch_data)
    dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

    state["running"] = True
    state["events"] = []

    background_tasks.add_task(
        run_dispatch, user_id, dispatch_id, contacts, req.template, cfg, api_url, api_key, instance
    )
    return {"status": "started", "total": len(contacts), "dispatch_id": dispatch_id}


@app.get("/api/dispatch/stream")
async def dispatch_stream(user_id: str = Depends(get_current_user)):
    state = get_user_state(user_id)
    queue: asyncio.Queue = asyncio.Queue()
    state["subscribers"].append(queue)

    for event in list(state["events"]):
        await queue.put(event)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("type") == "done":
                        break
                except asyncio.TimeoutError:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            if queue in state["subscribers"]:
                state["subscribers"].remove(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ----------------------------------------------------------------
# HISTORY
# ----------------------------------------------------------------
@app.get("/api/dispatches")
def list_dispatches(user_id: str = Depends(get_current_user)):
    rows = _db_get("dispatches", filters={"user_id": user_id}, order="created_at.desc")
    return {"dispatches": rows}


@app.get("/api/dispatches/{dispatch_id}/logs")
def get_dispatch_logs(dispatch_id: str, user_id: str = Depends(get_current_user)):
    dispatch = _db_get("dispatches", filters={"id": dispatch_id, "user_id": user_id}, columns="id")
    if not dispatch:
        raise HTTPException(404, "Disparo não encontrado.")
    logs = _db_get("dispatch_logs", filters={"dispatch_id": dispatch_id}, order="contact_index")
    return {"logs": logs}


# ================================================================
# OWNER MAPPING (Feature 1 — Fase 3)
# ================================================================

@app.get("/api/owner-mapping")
def list_owner_mapping(_: str = Depends(get_current_user)):
    rows = _db_get("owner_mapping", order="hubspot_owner_name")
    return {"mappings": rows}


@app.post("/api/owner-mapping")
def create_owner_mapping(req: OwnerMappingRequest, _: str = Depends(get_current_user)):
    try:
        inserted = _db_insert("owner_mapping", {
            "hubspot_owner_name": req.hubspot_owner_name,
            "adapta_email": req.adapta_email,
        })
        return {"status": "ok", "mapping": inserted[0] if inserted else {}}
    except Exception as e:
        raise HTTPException(500, f"Erro ao criar mapeamento: {str(e)}")


@app.delete("/api/owner-mapping/{mapping_id}")
def delete_owner_mapping(mapping_id: str, _: str = Depends(get_current_user)):
    try:
        _db_delete("owner_mapping", {"id": mapping_id})
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"Erro ao remover mapeamento: {str(e)}")


# ================================================================
# CLIENTES (Feature 2 — Fase 4)
# ================================================================

@app.get("/api/clients")
def list_clients(
    user_id: str = Depends(get_current_user),
    etapas: Optional[str] = Query(default=None),       # "Onboarding,Em andamento"
    dias_na_etapa: Optional[int] = Query(default=None), # mínimo de dias
    produto: Optional[str] = Query(default=None),
    sem_reuniao: Optional[bool] = Query(default=None),
    search: Optional[str] = Query(default=None),
):
    raw_params: dict = {"user_id": f"eq.{user_id}"}

    if etapas:
        etapa_list = etapas.split(",")
        raw_params["etapa_negocio"] = "in.(" + ",".join(etapa_list) + ")"

    if produto:
        raw_params["produto"] = f"eq.{produto}"

    if sem_reuniao:
        raw_params["data_reuniao_1"] = "is.null"

    if search:
        # Supabase não suporta OR direto via query params simples;
        # filtramos por nome (ilike) — suficiente para a maioria dos casos
        raw_params["nome"] = f"ilike.*{search}*"

    if dias_na_etapa is not None:
        # data_etapa_atual <= now() - interval 'X days'
        from datetime import timedelta
        cutoff = (datetime.utcnow() - timedelta(days=dias_na_etapa)).isoformat() + "Z"
        raw_params["data_etapa_atual"] = f"lte.{cutoff}"

    rows = _db_get(
        "hubspot_contacts",
        raw_params=raw_params,
        order="nome",
        columns="id,nome,primeiro_nome,telefone,email,etapa_negocio,produto,numero_consultoria,data_etapa_atual,data_reuniao_1",
    )

    # Calcula dias_na_etapa para cada contato
    now = datetime.utcnow()
    for row in rows:
        if row.get("data_etapa_atual"):
            try:
                dt = datetime.fromisoformat(row["data_etapa_atual"].replace("Z", "+00:00"))
                row["dias_na_etapa"] = (now - dt.replace(tzinfo=None)).days
            except Exception:
                row["dias_na_etapa"] = None
        else:
            row["dias_na_etapa"] = None

    return {"clients": rows, "total": len(rows)}


# ================================================================
# AGENDAMENTO (Feature 3 — Fase 5)
# ================================================================

async def check_scheduled_dispatches():
    """Job executado a cada minuto para disparar agendamentos pendentes."""
    try:
        now_iso = datetime.utcnow().isoformat() + "Z"
        pending = _db_get(
            "scheduled_dispatches",
            raw_params={"status": "eq.pending", "scheduled_at": f"lte.{now_iso}"},
        )
        for sched in pending:
            try:
                _db_patch("scheduled_dispatches", {"status": "running"}, {"id": sched["id"]})

                creds_rows = _db_get("user_credentials", filters={"user_id": sched["user_id"]})
                if not creds_rows:
                    _db_patch("scheduled_dispatches", {"status": "cancelled"}, {"id": sched["id"]})
                    continue

                cred = creds_rows[0]
                contacts = sched["contacts_json"] if isinstance(sched["contacts_json"], list) else json.loads(sched["contacts_json"])
                cfg = sched.get("lot_config") or {
                    "lot_every": 150, "lot_min": 500.0, "lot_max": 600.0,
                    "sub_every": 30, "sub_min": 158.0, "sub_max": 200.0,
                    "msg_min": 25.0, "msg_max": 50.0,
                }

                dispatch_data = {
                    "user_id": sched["user_id"],
                    "source_type": sched.get("dispatch_type", "bulk"),
                    "template": sched["template"],
                    "status": "running",
                    "total_contacts": len(contacts),
                    "lot_config": cfg,
                    "started_at": datetime.utcnow().isoformat() + "Z",
                }
                inserted = _db_insert("dispatches", dispatch_data)
                dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

                _db_patch("scheduled_dispatches", {"dispatch_id": dispatch_id}, {"id": sched["id"]})

                state = get_user_state(sched["user_id"])
                state["running"] = True
                state["events"] = []

                asyncio.create_task(run_dispatch(
                    sched["user_id"], dispatch_id, contacts, sched["template"], cfg,
                    cred["evolution_api_url"], cred["evolution_api_key"], cred["instance_name"],
                ))

                _db_patch("scheduled_dispatches", {"status": "done"}, {"id": sched["id"]})
            except Exception:
                _db_patch("scheduled_dispatches", {"status": "cancelled"}, {"id": sched["id"]})
    except Exception:
        pass


@app.post("/api/dispatch/schedule")
def schedule_dispatch(req: ScheduleDispatchRequest, user_id: str = Depends(get_current_user)):
    try:
        cfg = {
            "lot_every": req.lot_every, "lot_min": req.lot_min, "lot_max": req.lot_max,
            "sub_every": req.sub_every, "sub_min": req.sub_min, "sub_max": req.sub_max,
            "msg_min": req.msg_min, "msg_max": req.msg_max,
        }
        inserted = _db_insert("scheduled_dispatches", {
            "user_id": user_id,
            "scheduled_at": req.scheduled_at,
            "dispatch_type": req.dispatch_type,
            "contacts_json": req.contacts_json,
            "template": req.template,
            "lot_config": cfg,
        })
        return {"status": "scheduled", "id": inserted[0]["id"]}
    except Exception as e:
        raise HTTPException(500, f"Erro ao agendar disparo: {str(e)}")


@app.get("/api/dispatch/scheduled")
def list_scheduled(user_id: str = Depends(get_current_user)):
    rows = _db_get(
        "scheduled_dispatches",
        filters={"user_id": user_id},
        order="scheduled_at.desc",
        columns="id,scheduled_at,status,dispatch_type,template,created_at",
    )
    return {"scheduled": rows}


@app.delete("/api/dispatch/scheduled/{sched_id}")
def cancel_scheduled(sched_id: str, user_id: str = Depends(get_current_user)):
    rows = _db_get("scheduled_dispatches", filters={"id": sched_id, "user_id": user_id}, columns="id,status")
    if not rows:
        raise HTTPException(404, "Agendamento não encontrado.")
    if rows[0]["status"] != "pending":
        raise HTTPException(409, "Apenas agendamentos pendentes podem ser cancelados.")
    _db_patch("scheduled_dispatches", {"status": "cancelled"}, {"id": sched_id})
    return {"status": "cancelled"}


# ================================================================
# DISPARO PÓS CALL — CSAT (Feature 5 — Fase 6)
# ================================================================

def _build_csat_url(produto: str, contact: dict) -> str:
    """Gera a URL do CSAT substituindo as variáveis pelo produto."""
    template_url = CSAT_URLS.get((produto or "").upper(), "")
    if not template_url:
        return ""
    return (
        template_url
        .replace("{primeiro_nome}", contact.get("primeiro_nome") or contact.get("first_name") or "")
        .replace("{numero_consultoria}", contact.get("numero_consultoria") or "")
        .replace("{email}", contact.get("email") or "")
    )


def _render_postcall_msg(template: str, contact: dict) -> str:
    url = _build_csat_url(contact.get("produto", ""), contact)
    return (
        template
        .replace("{primeiro_nome}", contact.get("primeiro_nome") or contact.get("first_name") or "")
        .replace("{link_csat}", url)
        .replace("{nome}", contact.get("nome") or contact.get("name") or "")
    )


@app.post("/api/dispatch/postcall")
async def postcall_dispatch(
    req: PostCallDispatchRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    # Busca contatos pelo IDs fornecidos
    if not req.contact_ids:
        raise HTTPException(400, "Nenhum contato selecionado.")

    id_list = ",".join(req.contact_ids)
    rows = _db_get(
        "hubspot_contacts",
        raw_params={"id": f"in.({id_list})", "user_id": f"eq.{user_id}"},
        columns="id,nome,primeiro_nome,telefone,email,produto,numero_consultoria",
    )
    if not rows:
        raise HTTPException(404, "Contatos não encontrados.")

    # Monta lista de contatos no formato da engine de disparo
    contacts = []
    for row in rows:
        contacts.append({
            "name": row.get("nome") or "",
            "first_name": row.get("primeiro_nome") or "",
            "phone": row.get("telefone") or "",
            "email": row.get("email") or "",
            "produto": row.get("produto") or "",
            "numero_consultoria": row.get("numero_consultoria") or "",
        })

    # Se agendado, salva e retorna
    if req.scheduled_at:
        inserted = _db_insert("scheduled_dispatches", {
            "user_id": user_id,
            "scheduled_at": req.scheduled_at,
            "dispatch_type": "postcall",
            "contacts_json": contacts,
            "template": req.template,
        })
        return {"status": "scheduled", "id": inserted[0]["id"], "total": len(contacts)}

    # Disparo imediato
    state = get_user_state(user_id)
    if state["running"]:
        raise HTTPException(409, "Já existe um disparo em andamento.")

    creds_rows = _db_get("user_credentials", filters={"user_id": user_id})
    if not creds_rows:
        raise HTTPException(400, "Configure as credenciais primeiro em Configuração.")
    cred = creds_rows[0]

    cfg = {
        "lot_every": 150, "lot_min": 500.0, "lot_max": 600.0,
        "sub_every": 30, "sub_min": 158.0, "sub_max": 200.0,
        "msg_min": 25.0, "msg_max": 50.0,
    }

    dispatch_data = {
        "user_id": user_id,
        "source_type": "postcall",
        "template": req.template,
        "status": "running",
        "total_contacts": len(contacts),
        "lot_config": cfg,
        "started_at": datetime.utcnow().isoformat() + "Z",
    }
    inserted = _db_insert("dispatches", dispatch_data)
    dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

    # Pré-renderiza mensagens com URL CSAT por contato
    contacts_rendered = []
    for c in contacts:
        contacts_rendered.append({
            **c,
            "_rendered_msg": _render_postcall_msg(req.template, c),
        })

    state["running"] = True
    state["events"] = []

    # Usa engine existente mas sobrescreve render_msg para usar _rendered_msg
    async def run_postcall():
        total = len(contacts_rendered)
        ok = fail = 0
        loop = asyncio.get_event_loop()
        logs_batch = []

        for i, contact in enumerate(contacts_rendered, 1):
            msg = contact["_rendered_msg"]
            try:
                await loop.run_in_executor(
                    None, _send_sync, contact["phone"], msg,
                    cred["evolution_api_url"], cred["evolution_api_key"], cred["instance_name"],
                )
                status, err = "success", ""
                ok += 1
            except Exception as e:
                status = "error"
                err = str(e)[:120]
                fail += 1

            logs_batch.append({
                "dispatch_id": dispatch_id,
                "contact_index": i,
                "contact_name": contact["name"],
                "contact_phone": contact["phone"],
                "status": status,
                "error_message": err or None,
            })

            if len(logs_batch) >= 50:
                try:
                    _db_insert("dispatch_logs", logs_batch)
                except Exception:
                    pass
                logs_batch = []

            await _broadcast(user_id, {
                "type": "sent", "index": i, "total": total,
                "name": contact["name"], "phone": contact["phone"],
                "first": contact["first_name"], "status": status, "error": err,
                "ok": ok, "fail": fail,
            })

            if i < total:
                pause_secs, pause_reason = get_pause(i, cfg)
                for remaining in range(int(pause_secs), 0, -1):
                    await _broadcast(user_id, {
                        "type": "pause", "seconds": remaining,
                        "total_pause": int(pause_secs), "reason": pause_reason,
                    })
                    await asyncio.sleep(1)

        if logs_batch:
            try:
                _db_insert("dispatch_logs", logs_batch)
            except Exception:
                pass

        try:
            _db_patch("dispatches", {
                "status": "completed", "sent_count": ok, "error_count": fail,
                "finished_at": datetime.utcnow().isoformat() + "Z",
            }, {"id": dispatch_id})
        except Exception:
            pass

        state = get_user_state(user_id)
        state["running"] = False
        await _broadcast(user_id, {"type": "done", "ok": ok, "fail": fail})

    background_tasks.add_task(run_postcall)
    return {"status": "started", "total": len(contacts), "dispatch_id": dispatch_id}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("backend:app", host="0.0.0.0", port=port, reload=False)
