"""
Autenticação JWT local via PyJWT — sem I/O de rede por request.
"""

from typing import Optional

import jwt as pyjwt
from fastapi import Depends, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import ALLOWED_EMAIL_DOMAINS, SUPABASE_JWT_SECRET

security = HTTPBearer(auto_error=False)


def _decode_jwt_local(token: str) -> dict:
    """
    Valida JWT localmente (~0ms, sem I/O de rede).

    LIMITAÇÃO CONHECIDA: tokens revogados via Supabase Admin (logout forçado,
    troca de senha, suspensão de conta) continuam válidos até expirar naturalmente.
    Para sistema interno com usuários confiáveis: ACEITÁVEL.

    MITIGAÇÃO ATIVA: JWT_EXPIRY configurado para 900s (15 min) no Supabase.
    Após logout, o pior caso é 15 min de acesso residual.

    Para aumentar a segurança sem perder performance, uma opção futura é
    manter uma denylist em memória de tokens explicitamente revogados.
    """
    payload = pyjwt.decode(
        token,
        SUPABASE_JWT_SECRET,
        algorithms=["HS256"],
        audience="authenticated",
    )
    return {
        "id": payload["sub"],
        "email": payload.get("email", ""),
    }


async def _resolve_jwt(
    credentials: Optional[HTTPAuthorizationCredentials],
    token: Optional[str],
) -> dict:
    jwt_token = credentials.credentials if credentials else token
    if not jwt_token:
        raise HTTPException(status_code=401, detail="Token não fornecido.")
    try:
        user_data = _decode_jwt_local(jwt_token)
        email = user_data.get("email", "")
        if not any(email.endswith(d) for d in ALLOWED_EMAIL_DOMAINS):
            raise HTTPException(status_code=403, detail="Acesso restrito a emails corporativos.")
        return user_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Autenticação falhou: {str(e)}")


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    token: Optional[str] = Query(default=None),
) -> str:
    user_data = await _resolve_jwt(credentials, token)
    return user_data.get("id", "")


async def get_current_user_email(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    token: Optional[str] = Query(default=None),
) -> str:
    user_data = await _resolve_jwt(credentials, token)
    return user_data.get("email", "")
