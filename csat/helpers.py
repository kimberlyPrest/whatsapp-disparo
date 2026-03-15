"""
Disparo Pós-Call — CSAT (Feature 5).
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from auth import get_current_user
from config import CSAT_URLS, UTC
from database import _db_get_async, _db_insert_async
from dispatch.engine import run_dispatch
from state import get_user_state

router = APIRouter()
logger = logging.getLogger(__name__)


class PostCallDispatchRequest(BaseModel):
    contact_ids: list[str]
    template: str = "Olá {primeiro_nome}! Obrigado pela reunião. Por favor, responda nossa pesquisa: {link_csat}"
    scheduled_at: Optional[str] = None  # None = enviar agora


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


@router.post("/api/dispatch/postcall")
async def postcall_dispatch(
    req: PostCallDispatchRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_current_user),
):
    if not req.contact_ids:
        raise HTTPException(400, "Nenhum contato selecionado.")

    id_list = ",".join(req.contact_ids)
    rows = await _db_get_async(
        "hubspot_contacts",
        raw_params={"id": f"in.({id_list})", "user_id": f"eq.{user_id}"},
        columns="id,nome,primeiro_nome,telefone,email,produto,numero_consultoria",
    )
    if not rows:
        raise HTTPException(404, "Contatos não encontrados.")

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
        inserted = await _db_insert_async("scheduled_dispatches", {
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

    creds_rows = await _db_get_async("user_credentials", filters={"user_id": user_id})
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
        "started_at": datetime.now(UTC).isoformat(),
    }
    inserted = await _db_insert_async("dispatches", dispatch_data)
    dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

    # Pré-renderiza as mensagens CSAT por contato e injeta via render_fn
    contacts_rendered = [
        {**c, "_rendered_msg": _render_postcall_msg(req.template, c)}
        for c in contacts
    ]

    state["running"] = True
    state["events"] = []

    background_tasks.add_task(
        run_dispatch,
        user_id, dispatch_id, contacts_rendered, "",
        cfg,
        cred["evolution_api_url"], cred["evolution_api_key"], cred["instance_name"],
        render_fn=lambda c: c["_rendered_msg"],
    )
    return {"status": "started", "total": len(contacts), "dispatch_id": dispatch_id}
