"""
Webhook HubSpot + endpoints de inspeção do buffer de contatos.
"""

import asyncio
import hashlib
import hmac
import json
import time

from fastapi import APIRouter, Depends, HTTPException, Request

from auth import get_current_user
from config import HUBSPOT_CLIENT_SECRET
from hubspot.parser import _parse_hubspot_payload
from hubspot.upsert import _upsert_hubspot_contact_async

router = APIRouter()

# Buffers em memória (locais a este módulo)
_webhook_contacts: list = []
_webhook_raw: list = []


# ----------------------------------------------------------------
# HUBSPOT WEBHOOK — public endpoint (no auth required, single shared pool)
# URL: POST /api/webhook/hubspot
# ----------------------------------------------------------------
@router.post("/api/webhook/hubspot")
async def hubspot_webhook(request: Request):
    raw_body = await request.body()

    # Verificação de assinatura HMAC (HubSpot v1 ou v2)
    if HUBSPOT_CLIENT_SECRET:
        sig = request.headers.get("X-HubSpot-Signature", "")
        sig_version = request.headers.get("X-HubSpot-Signature-Version", "v1")
        body_str = raw_body.decode("utf-8")

        if sig_version == "v2":
            timestamp = request.headers.get("X-HubSpot-Request-Timestamp", "")
            try:
                # Rejeita requests com mais de 5 minutos (proteção contra replay)
                if abs(time.time() * 1000 - int(timestamp)) > 300_000:
                    raise HTTPException(401, "Timestamp do webhook expirado.")
            except (ValueError, TypeError):
                raise HTTPException(401, "Timestamp do webhook inválido.")
            source = HUBSPOT_CLIENT_SECRET + "POST" + str(request.url) + body_str + timestamp
        else:
            source = HUBSPOT_CLIENT_SECRET + body_str

        expected = hashlib.sha256(source.encode()).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(401, "Assinatura do webhook inválida.")

    try:
        body = json.loads(raw_body)
    except Exception:
        raise HTTPException(400, "Payload JSON inválido.")

    # Store raw payload for schema inspection (keep last 50)
    items = body if isinstance(body, list) else [body]
    _webhook_raw.extend(items)
    if len(_webhook_raw) > 50:
        del _webhook_raw[:-50]

    new_contacts = _parse_hubspot_payload(body)

    # Backward compat: mantém buffer em memória para disparo direto
    _webhook_contacts.extend(new_contacts)

    # Fase 2: persiste/atualiza no banco de forma assíncrona (não bloqueia a resposta)
    for contact in new_contacts:
        asyncio.create_task(_upsert_hubspot_contact_async(contact))

    return {"received": True, "parsed": len(new_contacts)}


@router.get("/api/webhook-contacts")
def get_webhook_contacts(_: str = Depends(get_current_user)):
    return {"contacts": list(_webhook_contacts), "count": len(_webhook_contacts)}


@router.delete("/api/webhook-contacts")
def clear_webhook_contacts(_: str = Depends(get_current_user)):
    _webhook_contacts.clear()
    return {"status": "cleared"}


@router.get("/api/webhook/hubspot/raw")
def get_webhook_raw(_: str = Depends(get_current_user)):
    """Returns raw HubSpot payloads for schema inspection."""
    all_keys: dict = {}
    for item in _webhook_raw:
        for k, v in item.items():
            if k not in all_keys:
                all_keys[k] = type(v).__name__
    return {
        "count": len(_webhook_raw),
        "all_keys_seen": all_keys,
        "samples": _webhook_raw[-3:],
    }
