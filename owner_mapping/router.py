"""
Rotas de mapeamento HubSpot owner_id → usuário Adapta.
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth import get_current_user, get_current_user_email
from database import _db_delete, _db_get, _db_insert, _db_patch
from state import _check_rate_limit

router = APIRouter()


class OwnerMappingRequest(BaseModel):
    hubspot_owner_id: str   # ID numérico do proprietário no HubSpot (ex: "81963654")
    adapta_email: str


class OwnerNameRequest(BaseModel):
    email: str


def _get_user_role(email: str) -> str:
    """Retorna 'superadmin', 'elite' ou 'geral' para o email informado."""
    try:
        rows = _db_get(
            "owner_mapping",
            raw_params={"adapta_email": f"eq.{email.lower()}"},
            columns="role",
        )
        return rows[0]["role"] if rows else "geral"
    except Exception:
        return "geral"


@router.get("/api/me/role")
async def get_my_role(email: str = Depends(get_current_user_email)):
    return {"role": _get_user_role(email)}


@router.post("/api/auth/owner-name")
def lookup_owner_name(req: OwnerNameRequest, request: Request):
    """
    Retorna nome pré-preenchido para o formulário de cadastro.
    Rate-limited por IP (5 req / 15 min) para prevenir enumeração de emails.
    """
    ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(ip):
        raise HTTPException(429, "Muitas tentativas. Aguarde alguns minutos.")
    email = req.email.strip().lower()
    if not email:
        return {"nome": None}
    try:
        rows = _db_get(
            "owner_mapping",
            raw_params={"adapta_email": f"eq.{email}"},
            columns="nome",
        )
        return {"nome": rows[0]["nome"] if rows else None}
    except Exception:
        return {"nome": None}


@router.get("/api/owner-mapping")
def list_owner_mapping(_: str = Depends(get_current_user)):
    rows = _db_get("owner_mapping", order="adapta_email")
    return {"mappings": rows}


@router.post("/api/owner-mapping")
def create_owner_mapping(req: OwnerMappingRequest, _: str = Depends(get_current_user)):
    try:
        inserted = _db_insert("owner_mapping", {
            "hubspot_owner_id": req.hubspot_owner_id,
            "adapta_email": req.adapta_email,
        })
        return {"status": "ok", "mapping": inserted[0] if inserted else {}}
    except Exception as e:
        raise HTTPException(500, f"Erro ao criar mapeamento: {str(e)}")


@router.delete("/api/owner-mapping/{mapping_id}")
def delete_owner_mapping(mapping_id: str, _: str = Depends(get_current_user)):
    try:
        _db_delete("owner_mapping", {"id": mapping_id})
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"Erro ao remover mapeamento: {str(e)}")
