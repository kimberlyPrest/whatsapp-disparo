"""
VPS Dispatch Worker — consome dispatch_queue e processa disparos WhatsApp.
Processo standalone rodando na VPS com PostgreSQL próprio.

Uso:
    cd vps_queue
    pip install -r requirements.txt
    # copie o .env do projeto ou exporte as variáveis manualmente
    python worker.py
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import httpx
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
)
logger = logging.getLogger("vps_worker")

UTC = timezone.utc

# ================================================================
# CONFIG
# ================================================================

def _parse_pg_host(raw: str) -> tuple[str, int]:
    if ":" in raw:
        h, p = raw.rsplit(":", 1)
        return h, int(p)
    return raw, 5432


_pg_raw        = os.environ["POSTGRES_HOST"]
_PG_HOST, _PG_PORT = _parse_pg_host(_pg_raw)
_PG_USER       = os.environ["POSTGRES_USER"]
_PG_PASS       = os.environ["POSTGRES_PASSWORD"]
_PG_DB         = os.environ["POSTGRES_DB"]

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

RENDER_URL    = os.getenv("RENDER_URL", "http://localhost:8000").rstrip("/")
WORKER_SECRET = os.getenv("WORKER_SECRET", "")
WORKER_ID     = os.getenv("WORKER_ID", "worker-1")

HEARTBEAT_INTERVAL = 30   # segundos entre atualizações de heartbeat
POLL_INTERVAL      = 10   # segundos de espera quando a fila está vazia
CHECKPOINT_EVERY   = 50   # salva progresso a cada N contatos
PAUSE_BROADCAST_EVERY = 5 # transmite evento de pausa a cada N segundos

# ================================================================
# CONEXÃO VPS POSTGRESQL
# ================================================================

def _pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=_PG_HOST, port=_PG_PORT,
        user=_PG_USER, password=_PG_PASS,
        dbname=_PG_DB,
        connect_timeout=10,
        options="-c application_name=vps_worker",
    )


# ================================================================
# SUPABASE REST HELPERS
# ================================================================

def _sb_headers() -> dict:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _sb_patch(client: httpx.Client, table: str, data: dict, filters: dict) -> None:
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = client.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_sb_headers(), params=params, json=data,
    )
    resp.raise_for_status()


def _sb_insert_batch(client: httpx.Client, table: str, rows: list) -> None:
    resp = client.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_sb_headers(), json=rows,
    )
    resp.raise_for_status()


# ================================================================
# RENDER SSE BRIDGE (fire-and-forget)
# ================================================================

def _notify_render(client: httpx.Client, dispatch_id: str, user_id: str, event: dict) -> None:
    """Envia evento ao Render para redistribuir via SSE. Ignora falhas."""
    if not RENDER_URL or not WORKER_SECRET:
        return
    try:
        client.post(
            f"{RENDER_URL}/api/dispatch/{dispatch_id}/worker-event",
            headers={"X-Worker-Secret": WORKER_SECRET, "Content-Type": "application/json"},
            json={"user_id": user_id, **event},
            timeout=5.0,
        )
    except Exception as e:
        logger.debug(f"SSE bridge falhou (dispatch {dispatch_id}): {e}")


# ================================================================
# QUEUE HELPERS
# ================================================================

def poll_job(conn) -> dict | None:
    """Busca próximo job pendente com SKIP LOCKED para evitar duplicatas."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT * FROM dispatch_queue
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            "UPDATE dispatch_queue SET status='running', worker_id=%s, started_at=NOW() WHERE id=%s",
            (WORKER_ID, row["id"]),
        )
        conn.commit()
        return dict(row)


def update_checkpoint(conn, job_id: int, index: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE dispatch_queue SET last_sent_index=%s, last_heartbeat=NOW() WHERE id=%s",
            (index, job_id),
        )
        conn.commit()


def complete_job(conn, job_id: int, status: str = "completed") -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE dispatch_queue SET status=%s, completed_at=NOW() WHERE id=%s",
            (status, job_id),
        )
        conn.commit()


# ================================================================
# ENGINE DE MENSAGENS
# ================================================================

def _render_msg(template: str, contact: dict) -> str:
    mapping = {
        "{primeiro_nome}": contact.get("first_name", ""),
        "{nome}":          contact.get("name", ""),
        "{telefone}":      contact.get("phone", ""),
        "{email}":         contact.get("email", ""),
        "{produto}":       contact.get("produto", ""),
    }
    result = template
    for k, v in mapping.items():
        result = result.replace(k, v or "")
    return result


def _get_pause(index: int, cfg: dict) -> tuple[float, str]:
    if index % cfg["lot_every"] == 0:
        return (
            random.uniform(cfg["lot_min"], cfg["lot_max"]),
            f"Pausa de lote — {index} msgs enviadas",
        )
    if index % cfg["sub_every"] == 0:
        return (
            random.uniform(cfg["sub_min"], cfg["sub_max"]),
            f"Pausa de sublote — {index} msgs enviadas",
        )
    return random.uniform(cfg["msg_min"], cfg["msg_max"]), "Pausa individual"


def _send_msg(
    ev_client: httpx.Client,
    phone: str, message: str,
    api_url: str, api_key: str, instance: str,
) -> None:
    resp = ev_client.post(
        f"{api_url}/message/sendText/{instance}",
        json={"number": phone, "text": message},
        headers={"apikey": api_key, "Content-Type": "application/json"},
        timeout=30.0,
    )
    resp.raise_for_status()


# ================================================================
# PROCESSADOR DE JOB
# ================================================================

def process_job(conn, job: dict) -> None:
    payload     = job["payload"]
    dispatch_id = job["dispatch_id"]
    user_id     = job["user_id"]
    start_from  = job.get("last_sent_index") or 0

    contacts = payload["contacts"]
    template = payload["template"]
    cfg      = payload["cfg"]
    api_url  = payload["api_url"]
    api_key  = payload["api_key"]
    instance = payload["instance"]

    total  = len(contacts)
    ok = fail = 0
    logs_batch: list[dict] = []
    last_heartbeat = time.monotonic()

    logger.info(f"[{dispatch_id}] Iniciando: {total} contatos, start_from={start_from}")

    with (
        httpx.Client(timeout=15)  as sb_client,
        httpx.Client(timeout=35)  as ev_client,
        httpx.Client(timeout=5)   as render_client,
    ):
        # Marca dispatch como running no Supabase
        try:
            _sb_patch(sb_client, "dispatches", {"status": "running"}, {"id": dispatch_id})
        except Exception as e:
            logger.warning(f"[{dispatch_id}] Falha ao marcar running no Supabase: {e}")

        for i, contact in enumerate(contacts[start_from:], start_from + 1):
            # Heartbeat periódico na fila VPS
            if time.monotonic() - last_heartbeat >= HEARTBEAT_INTERVAL:
                try:
                    update_checkpoint(conn, job["id"], i - 1)
                    last_heartbeat = time.monotonic()
                except Exception as e:
                    logger.warning(f"[{dispatch_id}] Falha no heartbeat: {e}")

            msg = _render_msg(template, contact)
            try:
                _send_msg(ev_client, contact["phone"], msg, api_url, api_key, instance)
                status, err = "success", ""
                ok += 1
            except Exception as e:
                status, err = "error", str(e)[:120]
                fail += 1

            logs_batch.append({
                "dispatch_id":   dispatch_id,
                "contact_index": i,
                "contact_name":  contact["name"],
                "contact_phone": contact["phone"],
                "status":        status,
                "error_message": err or None,
            })

            _notify_render(render_client, dispatch_id, user_id, {
                "type": "sent", "index": i, "total": total,
                "name": contact["name"], "phone": contact["phone"],
                "first": contact.get("first_name", ""),
                "status": status, "error": err, "ok": ok, "fail": fail,
            })

            # Checkpoint a cada CHECKPOINT_EVERY contatos
            if len(logs_batch) >= CHECKPOINT_EVERY:
                try:
                    _sb_insert_batch(sb_client, "dispatch_logs", logs_batch)
                except Exception as e:
                    logger.error(f"[{dispatch_id}] Falha ao persistir lote de logs: {e}")
                logs_batch = []

                update_checkpoint(conn, job["id"], i)
                last_heartbeat = time.monotonic()
                try:
                    _sb_patch(sb_client, "dispatches", {"last_sent_index": i}, {"id": dispatch_id})
                except Exception as e:
                    logger.error(f"[{dispatch_id}] Falha ao salvar checkpoint Supabase: {e}")

            # Pausa entre mensagens (só se não for o último contato)
            if i < total:
                pause_secs, pause_reason = _get_pause(i, cfg)
                end_at = time.time() + pause_secs
                last_broadcast = 0.0
                while time.time() < end_at:
                    now = time.time()
                    remaining = max(0, int(end_at - now))
                    # Transmite evento de pausa a cada PAUSE_BROADCAST_EVERY segundos
                    if now - last_broadcast >= PAUSE_BROADCAST_EVERY:
                        _notify_render(render_client, dispatch_id, user_id, {
                            "type": "pause",
                            "seconds": remaining,
                            "total_pause": int(pause_secs),
                            "reason": pause_reason,
                        })
                        last_broadcast = now
                    time.sleep(1)

        # Flush logs restantes
        if logs_batch:
            try:
                _sb_insert_batch(sb_client, "dispatch_logs", logs_batch)
            except Exception as e:
                logger.error(f"[{dispatch_id}] Falha ao persistir logs finais: {e}")

        # Finaliza dispatch no Supabase
        try:
            _sb_patch(sb_client, "dispatches", {
                "status":          "completed",
                "sent_count":      ok,
                "error_count":     fail,
                "last_sent_index": total,
                "finished_at":     datetime.now(UTC).isoformat(),
            }, {"id": dispatch_id})
        except Exception as e:
            logger.error(f"[{dispatch_id}] Falha ao finalizar dispatch no Supabase: {e}")

        _notify_render(render_client, dispatch_id, user_id, {
            "type": "done", "ok": ok, "fail": fail,
        })

    logger.info(f"[{dispatch_id}] Concluído: {ok} enviados, {fail} erros")


# ================================================================
# LOOP PRINCIPAL
# ================================================================

def main() -> None:
    logger.info(
        f"Worker '{WORKER_ID}' iniciando — VPS PostgreSQL {_PG_HOST}:{_PG_PORT}/{_PG_DB}"
    )

    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = _pg_connect()
                logger.info("Conectado ao PostgreSQL da VPS.")

            job = poll_job(conn)
            if not job:
                time.sleep(POLL_INTERVAL)
                continue

            try:
                process_job(conn, job)
                complete_job(conn, job["id"], "completed")
            except Exception as e:
                logger.error(f"Job {job['id']} falhou: {e}", exc_info=True)
                try:
                    complete_job(conn, job["id"], "failed")
                    with httpx.Client(timeout=10) as c:
                        _sb_patch(c, "dispatches", {
                            "status":      "interrupted",
                            "finished_at": datetime.now(UTC).isoformat(),
                        }, {"id": job["dispatch_id"]})
                        _notify_render(c, job["dispatch_id"], job["user_id"], {
                            "type": "done", "ok": 0, "fail": 0, "error": str(e),
                        })
                except Exception:
                    pass

        except psycopg2.OperationalError as e:
            logger.error(f"Erro de conexão PostgreSQL: {e} — reconectando em 30s")
            try:
                conn and conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(30)

        except KeyboardInterrupt:
            logger.info("Worker interrompido.")
            break

        except Exception as e:
            logger.error(f"Erro inesperado no loop principal: {e}", exc_info=True)
            time.sleep(5)

    if conn and not conn.closed:
        conn.close()


if __name__ == "__main__":
    main()
