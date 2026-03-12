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
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.oauth2.service_account import Credentials
from pydantic import BaseModel
from supabase import Client, create_client

# ================================================================
# SUPABASE CONFIG
# ================================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://aucdgkmujqzhginokegx.supabase.co")
SUPABASE_SERVICE_KEY = os.getenv(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF1Y2Rna211anF6aGdpbm9rZWd4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzMyMzgwOCwiZXhwIjoyMDg4ODk5ODB9.VlbAvc2o2oOGsUTt7bQkkW93P8mQDHWJVdbveUfIA9s",
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

COL_NAME = "Nome"
COL_PHONE = "Telefone"

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


# ================================================================
# AUTH
# ================================================================
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    token: Optional[str] = Query(default=None),
) -> str:
    jwt = None
    if credentials:
        jwt = credentials.credentials
    elif token:
        jwt = token

    if not jwt:
        raise HTTPException(status_code=401, detail="Token não fornecido.")

    try:
        response = supabase.auth.get_user(jwt)
        user = response.user
        if not user:
            raise HTTPException(status_code=401, detail="Token inválido.")
        email = user.email or ""
        if not email.endswith("@adapta.org"):
            raise HTTPException(status_code=403, detail="Acesso restrito a emails @adapta.org.")
        return user.id
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
                supabase.table("dispatch_logs").insert(logs_batch).execute()
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
            supabase.table("dispatch_logs").insert(logs_batch).execute()
        except Exception:
            pass

    try:
        supabase.table("dispatches").update(
            {
                "status": "completed",
                "sent_count": ok,
                "error_count": fail,
                "finished_at": datetime.utcnow().isoformat(),
            }
        ).eq("id", dispatch_id).execute()
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


@app.get("/api/credentials")
def get_credentials(user_id: str = Depends(get_current_user)):
    result = supabase.table("user_credentials").select("*").eq("user_id", user_id).execute()
    if result.data:
        cred = dict(result.data[0])
        cred["has_google_credentials"] = bool(cred.get("google_credentials"))
        cred.pop("google_credentials", None)
        return cred
    return {}


@app.post("/api/credentials")
def save_credentials(req: CredentialsRequest, user_id: str = Depends(get_current_user)):
    data = {
        "user_id": user_id,
        "evolution_api_url": req.evolution_api_url,
        "evolution_api_key": req.evolution_api_key,
        "instance_name": req.instance_name,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if req.google_credentials is not None:
        data["google_credentials"] = req.google_credentials

    existing = supabase.table("user_credentials").select("id").eq("user_id", user_id).execute()
    if existing.data:
        supabase.table("user_credentials").update(data).eq("user_id", user_id).execute()
    else:
        data["created_at"] = datetime.utcnow().isoformat()
        supabase.table("user_credentials").insert(data).execute()
    return {"status": "ok"}


@app.post("/api/load")
def load_contacts_route(body: dict, user_id: str = Depends(get_current_user)):
    sheet_url = body.get("sheet_url", "")
    if not sheet_url:
        raise HTTPException(400, "sheet_url obrigatório.")
    cred_result = (
        supabase.table("user_credentials")
        .select("google_credentials")
        .eq("user_id", user_id)
        .execute()
    )
    if not cred_result.data or not cred_result.data[0].get("google_credentials"):
        raise HTTPException(400, "Configure as credenciais Google primeiro em Configuração.")
    google_creds = cred_result.data[0]["google_credentials"]
    try:
        contacts = load_from_sheet(sheet_url, google_creds)
        return {"contacts": contacts}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/upload-csv")
async def upload_csv(
    file: UploadFile = File(...), _: str = Depends(get_current_user)
):
    content = await file.read()
    try:
        contacts = parse_csv_contacts(content)
        return {"contacts": contacts, "filename": file.filename}
    except Exception as e:
        raise HTTPException(400, f"Erro ao processar CSV: {e}")


@app.post("/api/dispatch/start")
async def start_dispatch(
    req: DispatchRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    state = get_user_state(user_id)
    if state["running"]:
        raise HTTPException(409, "Disparo já em andamento.")

    cred_result = (
        supabase.table("user_credentials").select("*").eq("user_id", user_id).execute()
    )
    if not cred_result.data:
        raise HTTPException(400, "Configure as credenciais primeiro em Configuração.")
    cred = cred_result.data[0]
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
        "started_at": datetime.utcnow().isoformat(),
    }
    dispatch_result = supabase.table("dispatches").insert(dispatch_data).execute()
    dispatch_id = dispatch_result.data[0]["id"]

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


@app.get("/api/dispatches")
def list_dispatches(user_id: str = Depends(get_current_user)):
    result = (
        supabase.table("dispatches")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return {"dispatches": result.data}


@app.get("/api/dispatches/{dispatch_id}/logs")
def get_dispatch_logs(dispatch_id: str, user_id: str = Depends(get_current_user)):
    dispatch = (
        supabase.table("dispatches")
        .select("id")
        .eq("id", dispatch_id)
        .eq("user_id", user_id)
        .execute()
    )
    if not dispatch.data:
        raise HTTPException(404, "Disparo não encontrado.")
    logs = (
        supabase.table("dispatch_logs")
        .select("*")
        .eq("dispatch_id", dispatch_id)
        .order("contact_index")
        .execute()
    )
    return {"logs": logs.data}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("backend:app", host="0.0.0.0", port=port, reload=False)
