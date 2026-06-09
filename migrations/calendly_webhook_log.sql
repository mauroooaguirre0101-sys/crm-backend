-- Persistent Calendly webhook event log
-- Replaces the in-memory _calendlyWebhookLog (lost on every Railway deploy)
-- Run once in Supabase SQL editor

CREATE TABLE IF NOT EXISTS calendly_webhook_log (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type   TEXT,
  invitee_uri  TEXT,
  cliente_id   TEXT,
  status       TEXT,        -- created | rescheduled | reschedule_fallback_created | canceled | no_mapping | error
  invitee_name TEXT,
  invitee_email TEXT,
  webhook_token TEXT,
  call_id      TEXT,
  error_msg    TEXT
);

-- Index for quick lookups by cliente and time
CREATE INDEX IF NOT EXISTS idx_cwl_cliente_id   ON calendly_webhook_log (cliente_id);
CREATE INDEX IF NOT EXISTS idx_cwl_received_at  ON calendly_webhook_log (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_cwl_invitee_uri  ON calendly_webhook_log (invitee_uri);

-- Auto-delete entries older than 90 days to keep the table lean
-- (optional: run manually or via a pg_cron job if desired)
-- DELETE FROM calendly_webhook_log WHERE received_at < now() - INTERVAL '90 days';
