-- ================================================================
-- Migration 001: Follow-up System Tables
-- Execute no Supabase Studio > SQL Editor
-- ================================================================

-- Tabela de contatos persistidos do HubSpot
CREATE TABLE IF NOT EXISTS hubspot_contacts (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hubspot_id        TEXT UNIQUE,
  user_id           UUID REFERENCES auth.users(id) ON DELETE SET NULL,
  nome              TEXT,
  primeiro_nome     TEXT,
  telefone          TEXT,
  email             TEXT,
  etapa_negocio     TEXT,
  produto           TEXT,           -- ELITE | LABS | SCALE
  numero_consultoria TEXT,
  data_reuniao_1    TIMESTAMPTZ,
  data_reuniao_2    TIMESTAMPTZ,
  data_reuniao_3    TIMESTAMPTZ,
  csat_reuniao_1    TEXT,
  csat_reuniao_2    TEXT,
  csat_reuniao_3    TEXT,
  data_etapa_atual  TIMESTAMPTZ,
  raw_payload       JSONB,
  created_at        TIMESTAMPTZ DEFAULT now(),
  updated_at        TIMESTAMPTZ DEFAULT now()
);

-- Índices para filtros comuns
CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_user_id ON hubspot_contacts(user_id);
CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_etapa ON hubspot_contacts(etapa_negocio);
CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_produto ON hubspot_contacts(produto);
CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_data_etapa ON hubspot_contacts(data_etapa_atual);


-- Mapeamento: nome do proprietário no HubSpot → usuário do sistema
CREATE TABLE IF NOT EXISTS owner_mapping (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hubspot_owner_name  TEXT NOT NULL,
  adapta_email        TEXT NOT NULL,
  adapta_user_id      UUID,         -- preenchido automaticamente na primeira query
  created_at          TIMESTAMPTZ DEFAULT now()
);

-- Inserir os 7 mapeamentos iniciais
-- ATENÇÃO: substitua pelos nomes exatos como aparecem no HubSpot
INSERT INTO owner_mapping (hubspot_owner_name, adapta_email) VALUES
  ('Kimberly Prestes',  'kimberly@adapta.org'),
  ('Usuario 2',         'usuario2@adapta.org'),
  ('Usuario 3',         'usuario3@adapta.org'),
  ('Usuario 4',         'usuario4@adapta.org'),
  ('Usuario 5',         'usuario5@adapta.org'),
  ('Usuario 6',         'usuario6@adapta.org'),
  ('Usuario 7',         'usuario7@adapta.org')
ON CONFLICT DO NOTHING;


-- Agendamentos de disparo
CREATE TABLE IF NOT EXISTS scheduled_dispatches (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id         UUID NOT NULL,
  scheduled_at    TIMESTAMPTZ NOT NULL,
  status          TEXT DEFAULT 'pending',  -- pending | running | done | cancelled
  dispatch_type   TEXT DEFAULT 'bulk',     -- bulk | postcall
  contacts_json   JSONB NOT NULL,
  template        TEXT NOT NULL,
  lot_config      JSONB,
  dispatch_id     UUID,
  created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_dispatches_user_id ON scheduled_dispatches(user_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_dispatches_status ON scheduled_dispatches(status);
CREATE INDEX IF NOT EXISTS idx_scheduled_dispatches_scheduled_at ON scheduled_dispatches(scheduled_at);
