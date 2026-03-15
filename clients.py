"""
Clientes HTTP persistentes (connection pooling).
Declarados aqui como None; inicializados no lifespan de backend.py.
Sem dependências de outros módulos do projeto — base da hierarquia.
"""

from typing import Optional

import httpx

# Usado para envios à Evolution API (alta frequência — centenas de chamadas por disparo)
_evolution_client: Optional[httpx.AsyncClient] = None

# Usado para chamadas async ao Supabase REST (scheduler, webhook upsert, routes async)
_db_async_client: Optional[httpx.AsyncClient] = None

# Usado para chamadas síncronas ao Supabase REST (routes def — FastAPI roda em threadpool)
_db_sync_client: Optional[httpx.Client] = None
