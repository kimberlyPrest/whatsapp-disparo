-- ================================================================
-- Migration 003: pg_cron Keep-Alive + Cleanup Automático
-- ANTES de executar: habilitar pg_cron e pg_net no Supabase Dashboard
--   → Database → Extensions → buscar "pg_cron" → Enable
--   → Database → Extensions → buscar "pg_net"  → Enable
-- ================================================================

-- Keep-alive: pinga o Render a cada 10 min, das 08h às 22h (horário UTC)
-- Ajuste o horário conforme seu timezone (08h BRT = 11h UTC)
SELECT cron.schedule(
  'keep-alive-render',
  '*/10 11-1 * * *',
  $$SELECT net.http_get('https://whatsapp-bulksender.onrender.com/health')$$
);

-- Cleanup: dispatch_logs com mais de 60 dias (diariamente às 04h UTC)
SELECT cron.schedule(
  'cleanup-dispatch-logs',
  '0 7 * * *',
  $$DELETE FROM dispatch_logs WHERE created_at < NOW() - INTERVAL '60 days'$$
);

-- Cleanup: dispatches completos/interrompidos com mais de 90 dias (domingos às 04h UTC)
SELECT cron.schedule(
  'cleanup-old-dispatches',
  '0 7 * * 0',
  $$DELETE FROM dispatches
    WHERE created_at < NOW() - INTERVAL '90 days'
    AND status IN ('completed', 'interrupted')$$
);
