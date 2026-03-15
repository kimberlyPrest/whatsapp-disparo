"""
WhatsApp Bulk Sender — FastAPI Backend (Multi-tenant + Supabase)
Entry point: inicializa clientes HTTP, scheduler e registra routers.
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

import clients
import scheduler as sched_module
from clients_page.router import router as clients_page_router
from config import SUPABASE_ANON_KEY, SUPABASE_URL, UTC
from contacts.router import router as contacts_router
from credentials.router import router as credentials_router
from csat.helpers import router as csat_router
from database import _db_get, _db_patch
from dispatch.router import router as dispatch_router
from dispatch.schedule_router import router as schedule_router
from hubspot.router import router as hubspot_router
from owner_mapping.router import router as owner_mapping_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Inicializa clientes HTTP com connection pooling
    clients._evolution_client = httpx.AsyncClient(timeout=30)
    clients._db_async_client  = httpx.AsyncClient(timeout=15)
    clients._db_sync_client   = httpx.Client(timeout=15)

    sched_module.setup_scheduler()
    sched_module.scheduler.start()

    # Marca dispatches que estavam "running" ao reiniciar como "interrupted"
    try:
        stale = _db_get("dispatches", raw_params={"status": "eq.running"}, columns="id")
        for row in stale:
            _db_patch(
                "dispatches",
                {"status": "interrupted", "finished_at": datetime.now(UTC).isoformat()},
                {"id": row["id"]},
            )
    except Exception as e:
        logger.warning(f"Não foi possível limpar dispatches travados na inicialização: {e}")

    yield

    sched_module.scheduler.shutdown()
    if clients._evolution_client:
        await clients._evolution_client.aclose()
    if clients._db_async_client:
        await clients._db_async_client.aclose()
    if clients._db_sync_client:
        clients._db_sync_client.close()


app = FastAPI(title="WhatsApp Bulk Sender", lifespan=lifespan)

# CORS: como o frontend é servido pelo mesmo servidor FastAPI, todas as chamadas
# do browser são same-origin e não precisam de CORS.
# ALLOWED_ORIGINS é usado apenas se houver necessidade de cross-origin (ex: dev local).
_raw_origins = os.getenv("ALLOWED_ORIGINS", os.getenv("RENDER_EXTERNAL_URL", ""))
_cors_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

# ================================================================
# ROUTERS
# ================================================================
app.include_router(dispatch_router)
app.include_router(schedule_router)
app.include_router(hubspot_router)
app.include_router(contacts_router)
app.include_router(credentials_router)
app.include_router(owner_mapping_router)
app.include_router(clients_page_router)
app.include_router(csat_router)


# ================================================================
# ROTAS RAIZ
# ================================================================
@app.get("/health")
def health():
    """Health check para keep-alive (pg_cron) e Render."""
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}


@app.get("/")
def index():
    return FileResponse("index.html")


@app.get("/api/config/public")
def public_config():
    """Expõe configuração pública (sem secrets) para o frontend inicializar o Supabase client."""
    return {
        "supabase_url":  SUPABASE_URL,
        "supabase_anon": SUPABASE_ANON_KEY,
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("backend:app", host="0.0.0.0", port=port, reload=False)
