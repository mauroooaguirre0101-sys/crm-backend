CREATE TABLE IF NOT EXISTS call_reports (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  cliente_id  TEXT        NOT NULL,
  filename    TEXT        NOT NULL,
  analysis    TEXT        NOT NULL,
  note        TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_reports_cliente ON call_reports (cliente_id, created_at DESC);
