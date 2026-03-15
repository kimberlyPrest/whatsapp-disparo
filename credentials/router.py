"""
Rotas de credenciais da Evolution API e Google.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from auth import get_current_user
from config import UTC
from database import _db_get, _db_insert, _db_patch

router = APIRouter()


class CredentialsRequest(BaseModel):
    evolution_api_url: str
    evolution_api_key: str
    instance_name: str
    google_credentials: Optional[dict] = None


@router.get("/api/credentials")
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


@router.post("/api/credentials")
def save_credentials(req: CredentialsRequest, user_id: str = Depends(get_current_user)):
    try:
        data = {
            "user_id": user_id,
            "evolution_api_url": req.evolution_api_url,
            "evolution_api_key": req.evolution_api_key,
            "instance_name": req.instance_name,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        if req.google_credentials is not None:
            data["google_credentials"] = req.google_credentials

        existing = _db_get("user_credentials", filters={"user_id": user_id}, columns="id")
        if existing:
            _db_patch("user_credentials", data, {"user_id": user_id})
        else:
            data["created_at"] = datetime.now(UTC).isoformat()
            _db_insert("user_credentials", data)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar credenciais: {str(e)}")
