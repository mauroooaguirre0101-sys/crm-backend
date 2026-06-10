-- Per-negocio settings table
-- Stores calendar provider preference (calendly | ghl) per negocio.
-- Takes priority over GHL_NEGOCIO_IDS env var.
-- Run once in Supabase SQL editor.

CREATE TABLE IF NOT EXISTS negocio_settings (
  negocio_id       TEXT        PRIMARY KEY,
  calendar_provider TEXT       NOT NULL DEFAULT 'calendly',  -- 'calendly' | 'ghl'
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Set cliente_4 to use GHL (Calendly free plan doesn't support webhooks)
INSERT INTO negocio_settings (negocio_id, calendar_provider)
VALUES ('cliente_4', 'ghl')
ON CONFLICT (negocio_id) DO UPDATE
  SET calendar_provider = 'ghl',
      updated_at        = now();
