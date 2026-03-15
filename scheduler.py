"""
Scheduler APScheduler + jobs periódicos.
setup_scheduler() é chamado no lifespan de backend.py antes de scheduler.start().
"""

import asyncio
import json
import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import UTC
from database import _db_get_async, _db_insert_async, _db_patch_async
from state import cleanup_volatile_state, get_user_state

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")


async def check_scheduled_dispatches():
    """Job executado a cada minuto para disparar agendamentos pendentes."""
    # Import local para evitar importação circular no topo do módulo
    from dispatch.engine import run_dispatch

    try:
        now_iso = datetime.now(UTC).isoformat()
        pending = await _db_get_async(
            "scheduled_dispatches",
            raw_params={"status": "eq.pending", "scheduled_at": f"lte.{now_iso}"},
        )
        for sched in pending:
            try:
                await _db_patch_async("scheduled_dispatches", {"status": "running"}, {"id": sched["id"]})

                creds_rows = await _db_get_async("user_credentials", filters={"user_id": sched["user_id"]})
                if not creds_rows:
                    await _db_patch_async("scheduled_dispatches", {"status": "cancelled"}, {"id": sched["id"]})
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
                    "started_at": datetime.now(UTC).isoformat(),
                }
                inserted = await _db_insert_async("dispatches", dispatch_data)
                dispatch_id = inserted[0]["id"] if isinstance(inserted, list) else inserted["id"]

                await _db_patch_async("scheduled_dispatches", {"dispatch_id": dispatch_id}, {"id": sched["id"]})

                state = get_user_state(sched["user_id"])
                state["running"] = True
                state["events"] = []

                # Salvar contatos no dispatch para possibilitar resume
                await _db_patch_async(
                    "dispatches",
                    {"contacts_json": contacts, "last_sent_index": 0},
                    {"id": dispatch_id},
                )

                # Wrapper: marca "done" APÓS o dispatch completar (não bloqueia o scheduler)
                async def dispatch_and_mark_done(
                    _sched_id=sched["id"],
                    _user_id=sched["user_id"],
                    _dispatch_id=dispatch_id,
                    _contacts=contacts,
                    _template=sched["template"],
                    _cfg=cfg,
                    _api_url=cred["evolution_api_url"],
                    _api_key=cred["evolution_api_key"],
                    _instance=cred["instance_name"],
                ):
                    try:
                        await run_dispatch(
                            _user_id, _dispatch_id, _contacts, _template, _cfg,
                            _api_url, _api_key, _instance,
                        )
                        await _db_patch_async("scheduled_dispatches", {"status": "done"}, {"id": _sched_id})
                    except Exception as exc:
                        logger.error(f"Erro no dispatch agendado {_sched_id}: {exc}")
                        await _db_patch_async("scheduled_dispatches", {"status": "cancelled"}, {"id": _sched_id})

                asyncio.create_task(dispatch_and_mark_done())
            except Exception as e:
                logger.error(f"Erro ao executar agendamento {sched.get('id')}: {e}")
                await _db_patch_async("scheduled_dispatches", {"status": "cancelled"}, {"id": sched["id"]})
    except Exception as e:
        logger.error(f"Erro no job check_scheduled_dispatches: {e}")


def setup_scheduler():
    """Registra os jobs periódicos. Chamado antes de scheduler.start()."""
    scheduler.add_job(check_scheduled_dispatches, "interval", minutes=1, id="check_scheduled")
    scheduler.add_job(cleanup_volatile_state, "interval", minutes=15, id="cleanup_rate_store")
