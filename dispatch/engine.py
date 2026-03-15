"""
Engine de disparo de mensagens WhatsApp.
Loop principal (run_dispatch) rodado como background task.
"""

import asyncio
import logging
import random
from datetime import datetime

import clients
from config import UTC
from database import _db_insert_async, _db_patch_async
from state import _broadcast, get_user_state

logger = logging.getLogger(__name__)


def get_pause(index: int, cfg: dict) -> tuple[float, str]:
    if index % cfg["lot_every"] == 0:
        return random.uniform(cfg["lot_min"], cfg["lot_max"]), f"Pausa de lote — {index} msgs enviadas"
    if index % cfg["sub_every"] == 0:
        return random.uniform(cfg["sub_min"], cfg["sub_max"]), f"Pausa de sublote — {index} msgs enviadas"
    return random.uniform(cfg["msg_min"], cfg["msg_max"]), "Pausa individual"


def render_msg(template: str, contact: dict) -> str:
    mapping = {
        "{primeiro_nome}": contact.get("first_name", ""),
        "{nome}":          contact.get("name", ""),
        "{telefone}":      contact.get("phone", ""),
        "{email}":         contact.get("email", ""),
        "{produto}":       contact.get("produto", ""),
    }
    result = template
    for placeholder, value in mapping.items():
        result = result.replace(placeholder, value or "")
    return result


async def _send_async(phone: str, message: str, api_url: str, api_key: str, instance: str) -> dict:
    """Envia mensagem via Evolution API sem bloquear o event loop."""
    resp = await clients._evolution_client.post(
        f"{api_url}/message/sendText/{instance}",
        json={"number": phone, "text": message},
        headers={"apikey": api_key, "Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


async def run_dispatch(
    user_id: str,
    dispatch_id: str,
    contacts: list[dict],
    template: str,
    cfg: dict,
    api_url: str,
    api_key: str,
    instance: str,
    render_fn=None,
    start_from: int = 0,
) -> None:
    total = len(contacts)
    ok = fail = 0

    # Contabilizar envios anteriores (resume)
    if start_from > 0:
        logger.info(f"Retomando dispatch {dispatch_id} do índice {start_from}/{total}")

    logs_batch = []

    for i, contact in enumerate(contacts[start_from:], start_from + 1):
        msg = render_fn(contact) if render_fn else render_msg(template, contact)
        try:
            await _send_async(contact["phone"], msg, api_url, api_key, instance)
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
                await _db_insert_async("dispatch_logs", logs_batch)
            except Exception as e:
                logger.error(f"Falha ao persistir lote de logs (dispatch {dispatch_id}): {e}")
            logs_batch = []

            # Checkpoint: salvar progresso a cada 50 contatos
            try:
                await _db_patch_async("dispatches", {"last_sent_index": i}, {"id": dispatch_id})
            except Exception as e:
                logger.error(f"Falha ao salvar checkpoint (dispatch {dispatch_id}, index {i}): {e}")

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
            await _db_insert_async("dispatch_logs", logs_batch)
        except Exception as e:
            logger.error(f"Falha ao persistir logs finais (dispatch {dispatch_id}): {e}")

    try:
        await _db_patch_async(
            "dispatches",
            {
                "status": "completed",
                "sent_count": ok,
                "error_count": fail,
                "last_sent_index": total,
                "finished_at": datetime.now(UTC).isoformat(),
            },
            {"id": dispatch_id},
        )
    except Exception as e:
        logger.error(f"Falha ao atualizar status final do dispatch {dispatch_id}: {e}")

    state = get_user_state(user_id)
    state["running"] = False
    await _broadcast(user_id, {"type": "done", "ok": ok, "fail": fail})
    # Libera memória após todos os subscribers receberem o evento "done"
    await asyncio.sleep(0.1)
    state["events"] = []
    state["subscribers"] = []
