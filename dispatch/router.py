"""
Rotas de disparo imediato e SSE.
"""

import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel

from auth import _resolve_jwt, get_current_user, security
from contacts.parsers import load_from_sheet
from database import _db_get_async, _db_insert_async, _db_patch_async
from dispatch.engine import run_dispatch
from state import _broadcast, consume_stream_token, generate_stream_token, get_user_state

logger = logging.getLogger(__name__)

MAX_CONTACTS = 4000


# ================================================================
# VPS QUEUE HELPERS
# ================================================================

def _enqueue_vps_sync(dispatch_id: str, user_id: str, payload: dict) -> None:
    """Insere job na fila VPS PostgreSQL (síncrono — chamado via asyncio.to_thread)."""
    import psycopg2
    import psycopg2.extras
    from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB, connect_timeout=10,
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO dispatch_queue (dispatch_id, user_id, payload)
                   VALUES (%s, %s, %s)""",
                (dispatch_id, user_id, psycopg2.extras.Json(payload)),
            )
    conn.close()


def _update_vps_start_from(dispatch_id: str, start_from: int) -> None:
    """Atualiza last_sent_index no job da fila para que o worker retome do checkpoint."""
    import psycopg2
    from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB, connect_timeout=10,
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dispatch_queue SET last_sent_index=%s WHERE dispatch_id=%s AND status='pending'",
                (start_from, dispatch_id),
            )
    conn.close()


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


router = APIRouter()


@router.post("/api/dispatch/start")
async def start_dispatch(
    req: DispatchRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    from datetime import datetime
    from hubspot.router import _webhook_contacts
    from config import UTC

    state = get_user_state(user_id)
    if state["running"]:
        raise HTTPException(409, "Disparo já em andamento.")

    rows = await _db_get_async("user_credentials", filters={"user_id": user_id})
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

    if len(contacts) > MAX_CONTACTS:
        raise HTTPException(400, f"Máximo de {MAX_CONTACTS} contatos por disparo.")

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
        "contacts_json": contacts,
        "last_sent_index": 0,
        "started_at": datetime.now(UTC).isoformat(),
    }
    inserted = await _db_insert_async("dispatches", dispatch_data)
    dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

    state["running"] = True
    state["events"] = []

    from config import VPS_QUEUE_ENABLED
    if VPS_QUEUE_ENABLED:
        vps_payload = {
            "contacts": contacts, "template": req.template, "cfg": cfg,
            "api_url": api_url, "api_key": api_key, "instance": instance,
        }
        try:
            await asyncio.to_thread(_enqueue_vps_sync, dispatch_id, user_id, vps_payload)
            return {"status": "queued", "total": len(contacts), "dispatch_id": dispatch_id}
        except Exception as e:
            logger.error(f"Falha ao enfileirar na VPS: {e} — executando localmente")

    background_tasks.add_task(
        run_dispatch, user_id, dispatch_id, contacts, req.template, cfg, api_url, api_key, instance
    )
    return {"status": "started", "total": len(contacts), "dispatch_id": dispatch_id}


@router.post("/api/dispatch/{dispatch_id}/resume")
async def resume_dispatch(
    dispatch_id: str,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    """Retoma um disparo interrompido a partir do último checkpoint."""
    from datetime import datetime
    from config import UTC

    state = get_user_state(user_id)
    if state["running"]:
        raise HTTPException(409, "Já há um disparo em andamento.")

    rows = await _db_get_async(
        "dispatches",
        raw_params={"id": f"eq.{dispatch_id}", "user_id": f"eq.{user_id}"},
    )
    if not rows:
        raise HTTPException(404, "Disparo não encontrado.")

    dispatch = rows[0]
    if dispatch["status"] != "interrupted":
        raise HTTPException(400, "Somente dispatches interrompidos podem ser retomados.")

    contacts = dispatch.get("contacts_json")
    if not contacts:
        raise HTTPException(400, "Contatos não disponíveis para retomar (dispatch antigo sem checkpoint).")

    start_from = dispatch.get("last_sent_index", 0)
    if start_from >= len(contacts):
        raise HTTPException(400, "Disparo já completou todos os contatos.")

    cred_rows = await _db_get_async("user_credentials", filters={"user_id": user_id})
    if not cred_rows:
        raise HTTPException(400, "Configure as credenciais primeiro.")
    cred = cred_rows[0]

    cfg = dispatch.get("lot_config") or {
        "lot_every": 150, "lot_min": 500.0, "lot_max": 600.0,
        "sub_every": 30, "sub_min": 158.0, "sub_max": 200.0,
        "msg_min": 25.0, "msg_max": 50.0,
    }

    # Atualizar status para running
    await _db_patch_async(
        "dispatches",
        {"status": "running", "started_at": datetime.now(UTC).isoformat()},
        {"id": dispatch_id},
    )

    state["running"] = True
    state["events"] = []

    from config import VPS_QUEUE_ENABLED
    if VPS_QUEUE_ENABLED:
        vps_payload = {
            "contacts": contacts, "template": dispatch["template"], "cfg": cfg,
            "api_url": cred["evolution_api_url"],
            "api_key": cred["evolution_api_key"],
            "instance": cred["instance_name"],
        }
        try:
            await asyncio.to_thread(_enqueue_vps_sync, dispatch_id, user_id, vps_payload)
            # Grava start_from no job da fila para o worker retomar do ponto correto
            await asyncio.to_thread(_update_vps_start_from, dispatch_id, start_from)
            remaining = len(contacts) - start_from
            return {
                "status": "queued", "dispatch_id": dispatch_id,
                "resumed_from": start_from, "remaining": remaining, "total": len(contacts),
            }
        except Exception as e:
            logger.error(f"Falha ao enfileirar resume na VPS: {e} — executando localmente")

    background_tasks.add_task(
        run_dispatch, user_id, dispatch_id, contacts, dispatch["template"], cfg,
        cred["evolution_api_url"], cred["evolution_api_key"], cred["instance_name"],
        None, start_from,
    )

    remaining = len(contacts) - start_from
    return {
        "status": "resumed",
        "dispatch_id": dispatch_id,
        "resumed_from": start_from,
        "remaining": remaining,
        "total": len(contacts),
    }


@router.post("/api/dispatch/stream-token")
async def get_stream_token(user_id: str = Depends(get_current_user)):
    """Gera um token de curta duração (30s) para autenticar a conexão SSE.
    Evita expor o JWT na URL do EventSource (que apareceria em logs de servidor).
    """
    token = generate_stream_token(user_id)
    return {"stream_token": token}


@router.get("/api/dispatch/stream")
async def dispatch_stream(
    stream_token: Optional[str] = Query(default=None),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    token: Optional[str] = Query(default=None),
):
    if stream_token:
        user_id = consume_stream_token(stream_token)
        if not user_id:
            raise HTTPException(401, "Stream token inválido ou expirado.")
    else:
        user_data = await _resolve_jwt(credentials, token)
        user_id = user_data.get("id", "")

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


@router.post("/api/dispatch/{dispatch_id}/worker-event")
async def worker_event(dispatch_id: str, request: Request):
    """Recebe eventos do VPS worker e redistribui via SSE para os subscribers do browser.
    Autenticado pelo header X-Worker-Secret.
    """
    from config import WORKER_SECRET

    if not WORKER_SECRET:
        raise HTTPException(403, "Worker-event endpoint desabilitado (WORKER_SECRET não configurado).")

    secret = request.headers.get("X-Worker-Secret", "")
    if secret != WORKER_SECRET:
        raise HTTPException(403, "Worker secret inválido.")

    payload = await request.json()
    user_id = payload.pop("user_id", None)
    if not user_id:
        raise HTTPException(400, "user_id obrigatório no payload.")

    state = get_user_state(user_id)
    event_type = payload.get("type")
    if event_type == "done":
        state["running"] = False
        state["events"] = []
    elif event_type in ("sent", "pause"):
        state["running"] = True

    await _broadcast(user_id, payload)
    return {"ok": True}
