-- ============================================================
-- calls_ghl: raw GHL payload store for cliente_2
-- Run in Supabase SQL Editor
-- ============================================================

CREATE TABLE IF NOT EXISTS calls_ghl (
  id                 BIGSERIAL PRIMARY KEY,
  cliente_id         TEXT        NOT NULL,
  call_id            BIGINT,                    -- FK to calls.id (nullable — set after upsert)
  contact_id         TEXT,
  first_name         TEXT,
  last_name          TEXT,
  full_name          TEXT,
  email              TEXT,
  phone              TEXT,
  calendar           JSONB,                     -- body.calendar embedded object
  workflow           JSONB,                     -- body.workflow embedded object
  trigger_data       JSONB,                     -- body.triggerData
  location           JSONB,                     -- body.location
  attribution_source JSONB,                     -- body.attributionSource
  custom_data        JSONB,                     -- body.customData
  raw_payload        JSONB,                     -- full body as received
  event_type         TEXT,                      -- AppointmentCreate / AppointmentUpdate / AppointmentDelete
  inferred           BOOLEAN     DEFAULT FALSE, -- true when eventType was inferred (no type field in payload)
  created_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_calls_ghl_cliente_id ON calls_ghl(cliente_id);
CREATE INDEX IF NOT EXISTS idx_calls_ghl_contact_id ON calls_ghl(contact_id);
CREATE INDEX IF NOT EXISTS idx_calls_ghl_call_id    ON calls_ghl(call_id);
CREATE INDEX IF NOT EXISTS idx_calls_ghl_created_at ON calls_ghl(created_at DESC);

-- Columns on calls table (add if not present)
ALTER TABLE calls ADD COLUMN IF NOT EXISTS provider_event_id TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS calendar_name      TEXT;
