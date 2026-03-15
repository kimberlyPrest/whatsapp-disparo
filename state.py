"""
Estado volátil em memória (por processo): dispatches, stream tokens, rate limiter.
Sem dependências de outros módulos do projeto.
"""

import asyncio
import secrets
from collections import defaultdict
from time import monotonic
from typing import Optional

# ================================================================
# DISPATCH STATE — { user_id: { running, events, subscribers } }
# ================================================================
_dispatches: dict[str, dict] = {}

# ================================================================
# STREAM TOKENS — token → (user_id, expiry_monotonic)
# Evita expor o JWT na URL do EventSource (apareceria em logs de servidor)
# ================================================================
_stream_tokens: dict[str, tuple[str, float]] = {}

# ================================================================
# RATE LIMITER — in-memory, para endpoints públicos sensíveis
# ================================================================
_rate_store: dict[str, list[float]] = defaultdict(list)
_RATE_WINDOW = 900   # 15 minutos
_RATE_MAX    = 5     # máx 5 requests por janela por IP


# ================================================================
# DISPATCH HELPERS
# ================================================================

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
# STREAM TOKEN HELPERS
# ================================================================

def generate_stream_token(user_id: str) -> str:
    """Gera token de curta duração (30s) para autenticar conexão SSE."""
    token = secrets.token_urlsafe(32)
    _stream_tokens[token] = (user_id, monotonic() + 30)
    return token


def consume_stream_token(token: str) -> Optional[str]:
    """Consome o token e retorna user_id se válido, None se inválido/expirado."""
    entry = _stream_tokens.pop(token, None)
    if not entry or monotonic() > entry[1]:
        return None
    return entry[0]


# ================================================================
# RATE LIMITER
# ================================================================

def _check_rate_limit(ip: str) -> bool:
    now = monotonic()
    cutoff = now - _RATE_WINDOW
    hits = [t for t in _rate_store[ip] if t > cutoff]
    if not hits:
        _rate_store[ip] = [now]
        return True
    if len(hits) >= _RATE_MAX:
        _rate_store[ip] = hits
        return False
    _rate_store[ip] = hits + [now]
    return True


# ================================================================
# CLEANUP (executado pelo scheduler a cada 15 min)
# ================================================================

def cleanup_volatile_state() -> None:
    """Remove IPs sem hits na janela atual e stream tokens expirados."""
    now = monotonic()
    stale_ips = [
        ip for ip, hits in list(_rate_store.items())
        if not any(t > now - _RATE_WINDOW for t in hits)
    ]
    for ip in stale_ips:
        del _rate_store[ip]
    expired_tokens = [tok for tok, (_, exp) in list(_stream_tokens.items()) if now > exp]
    for tok in expired_tokens:
        del _stream_tokens[tok]
