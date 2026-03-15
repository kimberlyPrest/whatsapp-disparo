-- ================================================================
-- Migration 002: Checkpoint + Resume + sent_at
-- Execute no Supabase Studio > SQL Editor
-- ================================================================

-- Checkpoint: salva progresso do dispatch para retomar após interrupção
ALTER TABLE dispatches ADD COLUMN IF NOT EXISTS last_sent_index INTEGER DEFAULT 0;

-- Persiste os contatos no banco para possibilitar resume
ALTER TABLE dispatches ADD COLUMN IF NOT EXISTS contacts_json JSONB;

-- Horário de envio individual de cada log
ALTER TABLE dispatch_logs ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ DEFAULT now();
