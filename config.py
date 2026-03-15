"""
Configuração central — lida de variáveis de ambiente.
Sem dependências de outros módulos do projeto.
"""

import os
from datetime import timezone

# IMPORTANTE: o JWT_EXPIRY deve estar configurado para 900 (15 min) no
# Supabase Dashboard → Authentication → JWT expiry.
# Com expiração curta, tokens revogados ficam válidos por no máximo 15 min.

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SUPABASE_ANON_KEY    = os.environ["SUPABASE_ANON_KEY"]
SUPABASE_JWT_SECRET  = os.environ["SUPABASE_JWT_SECRET"]

# HubSpot webhook secret (opcional — se definido, verifica assinatura HMAC)
HUBSPOT_CLIENT_SECRET = os.getenv("HUBSPOT_CLIENT_SECRET")

ALLOWED_EMAIL_DOMAINS = ("@adapta.org", "@copyexperts.com.br")

# ================================================================
# VPS PostgreSQL (fila de dispatch — Fase 3)
# POSTGRES_HOST pode conter host:porta ou só host
# ================================================================
_pg_raw = os.getenv("POSTGRES_HOST", "")
if ":" in _pg_raw:
    _pg_host_only, _pg_port_str = _pg_raw.rsplit(":", 1)
    POSTGRES_HOST = _pg_host_only
    POSTGRES_PORT = int(_pg_port_str)
else:
    POSTGRES_HOST = _pg_raw
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORTA", "5432").strip() or "5432")

POSTGRES_USER     = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "")

VPS_QUEUE_ENABLED = bool(
    POSTGRES_HOST and POSTGRES_USER and POSTGRES_PASSWORD and POSTGRES_DB
)

# Segredo compartilhado entre Render e VPS worker para o endpoint /worker-event
WORKER_SECRET = os.getenv("WORKER_SECRET", "")

# URL base do Render (para o worker chamar de volta via HTTP)
RENDER_URL = os.getenv("RENDER_URL", "http://localhost:8000").rstrip("/")

COL_NAME  = "Nome"
COL_PHONE = "Telefone"

UTC = timezone.utc

# ================================================================
# CSAT URLs por produto (Feature 5)
# ================================================================
CSAT_URLS = {
    "ELITE": "https://tally.so/r/wdg6KD?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
    "LABS":  "https://tally.so/r/nre1xl?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
    "SCALE": "https://form.adapta.org/r/9qqReQ?firstname={primeiro_nome}&consultoria={numero_consultoria}&e-mail={email}",
}
