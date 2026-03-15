"""
Rotas de listagem de clientes (HubSpot contacts), histórico de dispatches e logs.
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_current_user
from config import UTC
from database import _db_get

router = APIRouter()


@router.get("/api/clients")
def list_clients(
    user_id: str = Depends(get_current_user),
    etapas: Optional[str] = Query(default=None),
    dias_na_etapa: Optional[int] = Query(default=None),
    produto: Optional[str] = Query(default=None),
    sem_reuniao: Optional[bool] = Query(default=None),
    search: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
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
        raw_params["nome"] = f"ilike.*{search}*"

    if dias_na_etapa is not None:
        cutoff = (datetime.now(UTC) - timedelta(days=dias_na_etapa)).isoformat()
        raw_params["data_etapa_atual"] = f"lte.{cutoff}"

    raw_params["limit"]  = page_size
    raw_params["offset"] = (page - 1) * page_size

    rows = _db_get(
        "hubspot_contacts",
        raw_params=raw_params,
        order="nome",
        columns="id,nome,primeiro_nome,telefone,email,etapa_negocio,produto,numero_consultoria,data_etapa_atual,data_reuniao_1",
    )

    now = datetime.now(UTC)
    for row in rows:
        if row.get("data_etapa_atual"):
            try:
                dt = datetime.fromisoformat(row["data_etapa_atual"].replace("Z", "+00:00"))
                row["dias_na_etapa"] = (now - dt).days
            except Exception:
                row["dias_na_etapa"] = None
        else:
            row["dias_na_etapa"] = None

    return {
        "clients": rows,
        "page": page,
        "page_size": page_size,
        "has_more": len(rows) == page_size,
    }


@router.get("/api/dispatches")
def list_dispatches(
    user_id: str = Depends(get_current_user),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    rows = _db_get(
        "dispatches",
        filters={"user_id": user_id},
        order="created_at.desc",
        raw_params={"limit": page_size, "offset": (page - 1) * page_size},
    )
    return {"dispatches": rows, "page": page, "page_size": page_size, "has_more": len(rows) == page_size}


@router.get("/api/dispatches/{dispatch_id}/logs")
def get_dispatch_logs(dispatch_id: str, user_id: str = Depends(get_current_user)):
    dispatch = _db_get("dispatches", filters={"id": dispatch_id, "user_id": user_id}, columns="id")
    if not dispatch:
        raise HTTPException(404, "Disparo não encontrado.")
    logs = _db_get("dispatch_logs", filters={"dispatch_id": dispatch_id}, order="contact_index")
    return {"logs": logs}
