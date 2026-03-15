"""
Helpers de acesso ao Supabase via HTTP direto (sem supabase-py).
Sync: usa clients._db_sync_client (httpx.Client)
Async: usa clients._db_async_client (httpx.AsyncClient)
"""

import clients
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY


def _db_headers(prefer: str = "return=representation") -> dict:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


# ================================================================
# SYNC HELPERS (para routes def — FastAPI roda em threadpool)
# ================================================================

def _db_get(
    table: str,
    filters: dict = None,
    columns: str = "*",
    order: str = None,
    raw_params: dict = None,
) -> list:
    """
    filters: {col: val} → col=eq.val
    raw_params: passthrough params para filtros avançados (in, gte, ilike, etc.)
    """
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params: dict = {"select": columns}
    if filters:
        for k, v in filters.items():
            params[k] = f"eq.{v}"
    if raw_params:
        params.update(raw_params)
    if order:
        params["order"] = order
    resp = clients._db_sync_client.get(url, headers=_db_headers(), params=params)
    resp.raise_for_status()
    return resp.json()


def _db_upsert(table: str, data, on_conflict: str) -> list:
    """Upsert one or many rows. Returns list of upserted rows."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = _db_headers(prefer="resolution=merge-duplicates,return=representation")
    params = {"on_conflict": on_conflict}
    resp = clients._db_sync_client.post(url, headers=headers, json=data, params=params)
    resp.raise_for_status()
    return resp.json()


def _db_delete(table: str, filters: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = clients._db_sync_client.delete(url, headers=_db_headers(prefer="return=minimal"), params=params)
    resp.raise_for_status()


def _db_insert(table: str, data) -> list:
    """Insert one row (dict) or many rows (list of dicts). Returns list of inserted rows."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = clients._db_sync_client.post(url, headers=_db_headers(), json=data)
    resp.raise_for_status()
    return resp.json()


def _db_patch(table: str, data: dict, filters: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = clients._db_sync_client.patch(
        url, headers=_db_headers(prefer="return=minimal"), json=data, params=params
    )
    resp.raise_for_status()


# ================================================================
# ASYNC HELPERS (para coroutines: scheduler, webhook, routes async)
# ================================================================

async def _db_get_async(
    table: str,
    filters: dict = None,
    columns: str = "*",
    order: str = None,
    raw_params: dict = None,
) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params: dict = {"select": columns}
    if filters:
        for k, v in filters.items():
            params[k] = f"eq.{v}"
    if raw_params:
        params.update(raw_params)
    if order:
        params["order"] = order
    resp = await clients._db_async_client.get(url, headers=_db_headers(), params=params)
    resp.raise_for_status()
    return resp.json()


async def _db_patch_async(table: str, data: dict, filters: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = await clients._db_async_client.patch(url, headers=_db_headers(), json=data, params=params)
    resp.raise_for_status()


async def _db_insert_async(table: str, data) -> list:
    """Insert one row (dict) or many rows (list of dicts). Returns list of inserted rows."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = await clients._db_async_client.post(url, headers=_db_headers(), json=data)
    resp.raise_for_status()
    return resp.json()


async def _db_upsert_async(table: str, data, on_conflict: str) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {"on_conflict": on_conflict}
    resp = await clients._db_async_client.post(
        url,
        headers=_db_headers(prefer="resolution=merge-duplicates,return=representation"),
        json=data,
        params=params,
    )
    resp.raise_for_status()
    return resp.json()
