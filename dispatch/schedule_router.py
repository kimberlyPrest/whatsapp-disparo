"""
Rotas de agendamento de disparos.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from auth import get_current_user
from database import _db_get, _db_insert, _db_patch

router = APIRouter()


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


@router.post("/api/dispatch/schedule")
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


@router.get("/api/dispatch/scheduled")
def list_scheduled(user_id: str = Depends(get_current_user)):
    rows = _db_get(
        "scheduled_dispatches",
        filters={"user_id": user_id},
        order="scheduled_at.desc",
        columns="id,scheduled_at,status,dispatch_type,template,created_at",
    )
    return {"scheduled": rows}


@router.delete("/api/dispatch/scheduled/{sched_id}")
def cancel_scheduled(sched_id: str, user_id: str = Depends(get_current_user)):
    rows = _db_get("scheduled_dispatches", filters={"id": sched_id, "user_id": user_id}, columns="id,status")
    if not rows:
        raise HTTPException(404, "Agendamento não encontrado.")
    if rows[0]["status"] != "pending":
        raise HTTPException(409, "Apenas agendamentos pendentes podem ser cancelados.")
    _db_patch("scheduled_dispatches", {"status": "cancelled"}, {"id": sched_id})
    return {"status": "cancelled"}
