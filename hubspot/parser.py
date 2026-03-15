"""
Parser de payloads do webhook HubSpot.
"""

from datetime import datetime
from typing import Optional

from config import UTC
from contacts.parsers import normalize_phone


def _unix_ms_to_iso(value) -> Optional[str]:
    """Converte timestamp Unix em milissegundos para ISO 8601 timezone-aware."""
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC).isoformat()
    except Exception:
        return None


def _parse_hubspot_payload(payload) -> list[dict]:
    """
    Parse HubSpot webhook payload (formato flat confirmado via /raw).

    Campos confirmados no payload:
      nome, email, phone (int), data_1a_call (unix ms),
      hubspot_owner_id (int), csat_labs_enviado, csat_scale_enviado,
      comentario_csat_primeira_reuniao

    Campos a adicionar futuramente (quando o HubSpot enviar):
      produto, etapa_negocio, data_2a_call, data_3a_call, numero_consultoria
    """
    items = payload if isinstance(payload, list) else [payload]
    contacts = []

    for item in items:
        if not isinstance(item, dict):
            continue

        # — Campos flat (sem wrapper properties) —
        def _field(key):
            v = item.get(key)
            if v is None:
                return None
            return str(v).strip() if str(v).strip() else None

        # — Nome —
        name = _field("nome") or _field("name") or _field("firstname") or ""
        if not name:
            continue

        # — Telefone (pode vir como int ou str) —
        phone_raw = item.get("phone") or item.get("telefone") or item.get("mobilephone") or ""
        phone = str(phone_raw).strip() if phone_raw else ""
        if not phone:
            continue

        # — Owner ID (inteiro, não nome) —
        owner_id = item.get("hubspot_owner_id")
        owner_id_str = str(int(owner_id)) if owner_id is not None else None

        # — Datas (Unix ms → ISO) —
        data_reuniao_1 = _unix_ms_to_iso(item.get("data_1a_call"))
        data_reuniao_2 = _unix_ms_to_iso(item.get("data_2a_call"))    # campo futuro
        data_reuniao_3 = _unix_ms_to_iso(item.get("data_3a_call"))    # campo futuro

        # — Campos a adicionar futuramente —
        etapa_negocio    = _field("etapa_negocio") or _field("dealstage")
        produto          = _field("produto") or _field("product_line")
        numero_consultoria = _field("numero_consultoria") or _field("hs_object_id")
        data_etapa_atual = _unix_ms_to_iso(item.get("data_etapa_atual"))

        # — CSATs —
        csat_reuniao_1 = _field("csat_labs_enviado") or _field("csat_scale_enviado") or _field("csat_reuniao_1")
        csat_reuniao_2 = _field("csat_reuniao_2")
        csat_reuniao_3 = _field("comentario_csat_primeira_reuniao")

        # — HubSpot ID do contato/deal (usa email como fallback único) —
        hubspot_id = _field("hs_object_id") or _field("id")
        if not hubspot_id and item.get("email"):
            hubspot_id = f"email:{item['email']}"

        contacts.append({
            # campos para engine de disparo (compatibilidade existente)
            "name": name,
            "first_name": name.split()[0].capitalize(),
            "phone": normalize_phone(phone),
            # campos novos
            "email": _field("email"),
            "etapa_negocio": etapa_negocio,
            "produto": produto,
            "numero_consultoria": numero_consultoria,
            "owner_id": owner_id_str,
            "data_reuniao_1": data_reuniao_1,
            "data_reuniao_2": data_reuniao_2,
            "data_reuniao_3": data_reuniao_3,
            "csat_reuniao_1": csat_reuniao_1,
            "csat_reuniao_2": csat_reuniao_2,
            "csat_reuniao_3": csat_reuniao_3,
            "data_etapa_atual": data_etapa_atual,
            "hubspot_id": hubspot_id,
            "raw_payload": item,
        })

    return contacts
