"""
Upsert de contatos HubSpot no Supabase (async).
"""

import logging
from datetime import datetime
from typing import Optional

from config import UTC
from database import _db_get_async, _db_patch_async, _db_upsert_async

logger = logging.getLogger(__name__)


async def _resolve_owner_user_id_async(owner_id: str) -> Optional[str]:
    """Versão async de _resolve_owner_user_id — sem bloquear o event loop."""
    if not owner_id:
        return None
    try:
        rows = await _db_get_async(
            "owner_mapping",
            raw_params={"hubspot_owner_id": f"eq.{owner_id}"},
            columns="adapta_user_id,adapta_email",
        )
        if not rows:
            return None
        row = rows[0]
        if not row.get("adapta_user_id") and row.get("adapta_email"):
            auth_rows = await _db_get_async(
                "auth_users_view",
                raw_params={"email": f"eq.{row['adapta_email']}"},
                columns="id",
            )
            if auth_rows:
                uid = auth_rows[0]["id"]
                await _db_patch_async("owner_mapping", {"adapta_user_id": uid},
                                      {"hubspot_owner_id": owner_id})
                return uid
        return row.get("adapta_user_id")
    except Exception:
        return None


async def _upsert_hubspot_contact_async(contact: dict) -> None:
    """Persiste/atualiza contato HubSpot no banco de forma assíncrona."""
    if not contact.get("hubspot_id"):
        return
    try:
        user_id = await _resolve_owner_user_id_async(contact.get("owner_id"))
        row = {
            "hubspot_id": contact["hubspot_id"],
            "user_id": user_id,
            "nome": contact["name"],
            "primeiro_nome": contact["first_name"],
            "telefone": contact["phone"],
            "email": contact.get("email"),
            "etapa_negocio": contact.get("etapa_negocio"),
            "produto": contact.get("produto"),
            "numero_consultoria": contact.get("numero_consultoria"),
            "data_reuniao_1": contact.get("data_reuniao_1"),
            "data_reuniao_2": contact.get("data_reuniao_2"),
            "data_reuniao_3": contact.get("data_reuniao_3"),
            "csat_reuniao_1": contact.get("csat_reuniao_1"),
            "csat_reuniao_2": contact.get("csat_reuniao_2"),
            "csat_reuniao_3": contact.get("csat_reuniao_3"),
            "data_etapa_atual": contact.get("data_etapa_atual"),
            "raw_payload": contact.get("raw_payload"),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        await _db_upsert_async("hubspot_contacts", row, on_conflict="hubspot_id")
    except Exception as e:
        logger.error(f"Falha ao upsert contato HubSpot async {contact.get('hubspot_id')}: {e}")
