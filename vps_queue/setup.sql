-- ================================================================
-- VPS Queue Setup — Executar UMA VEZ no PostgreSQL da VPS
-- Conecte via: psql postgresql://USER:PASS@HOST:PORT/DB
-- ================================================================

-- Fila de dispatches para o worker consumir
CREATE TABLE IF NOT EXISTS dispatch_queue (
    id               BIGSERIAL PRIMARY KEY,
    dispatch_id      TEXT        NOT NULL,
    user_id          TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    status           VARCHAR(20) NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    worker_id        TEXT,
    last_heartbeat   TIMESTAMPTZ,
    last_sent_index  INTEGER     NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

-- Índice parcial: só rows pendentes são consultadas no hot path
CREATE INDEX IF NOT EXISTS idx_dq_pending ON dispatch_queue (created_at)
    WHERE status = 'pending';

-- Jobs travados (worker morreu sem completar) voltam para 'pending' após 30 min
-- Execute manualmente ou via pg_cron se instalado:
-- SELECT cron.schedule('requeue-stuck', '*/15 * * * *', $$
--   UPDATE dispatch_queue
--      SET status = 'pending', worker_id = NULL, last_heartbeat = NULL
--    WHERE status = 'running'
--      AND (last_heartbeat IS NULL OR last_heartbeat < NOW() - INTERVAL '30 minutes')
-- $$);
