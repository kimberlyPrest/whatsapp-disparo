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


def _db_get(table: str, filters: dict = None, columns: str = "*", order: str = None) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params: dict = {"select": columns}
    if filters:
        for k, v in filters.items():
            params[k] = f"eq.{v}"
    if order:
        params["order"] = order
    resp = requests.get(url, headers=_db_headers(), params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


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

# Shared HubSpot webhook contacts buffer (single pool, user division to be added later)
_webhook_contacts: list = []

# Raw payloads received from HubSpot (last 50) — for inspection/schema discovery
_webhook_raw: list = []


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
    Parse HubSpot webhook payload into [{name, first_name, phone}].
    Handles both single object and list. Supports:
      - Simple: {"nome": "...", "telefone": "..."}
      - HubSpot native: {"properties": {"firstname": {"value":"..."}, "phone": {"value":"..."}}}
      - HubSpot flat:   {"properties": {"firstname": "...", "phone": "..."}}
    """
    items = payload if isinstance(payload, list) else [payload]
    contacts = []

    for item in items:
        if not isinstance(item, dict):
            continue

        # Simple format: {"nome": "...", "telefone": "..."}
        name = str(item.get("nome") or item.get("name") or "").strip()
        phone = str(item.get("telefone") or item.get("phone") or "").strip()

        # HubSpot properties format
        if not name or not phone:
            props = item.get("properties", {})
            if isinstance(props, dict):
                def _prop(key):
                    v = props.get(key, "")
                    if isinstance(v, dict):
                        return str(v.get("value", "")).strip()
                    return str(v or "").strip()

                firstname = _prop("firstname")
                lastname = _prop("lastname")
                if firstname or lastname:
                    name = f"{firstname} {lastname}".strip()

                phone = (
                    _prop("mobilephone")
                    or _prop("phone")
                    or _prop("telefone")
                    or _prop("whatsapp")
                )

        name = name.strip()
        phone = phone.strip()

        if name and phone:
            contacts.append({
                "name": name,
                "first_name": name.split()[0].capitalize(),
                "phone": normalize_phone(phone),
            })

    return contacts


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
    _webhook_contacts.extend(new_contacts)
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("backend:app", host="0.0.0.0", port=port, reload=False)
