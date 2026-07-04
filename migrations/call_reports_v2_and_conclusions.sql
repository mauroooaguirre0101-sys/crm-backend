-- Hacer analysis nullable y agregar columna pdf_base64 en call_reports
ALTER TABLE call_reports ALTER COLUMN analysis DROP NOT NULL;
ALTER TABLE call_reports ADD COLUMN IF NOT EXISTS pdf_base64 TEXT;

-- Historial de conclusiones generales
CREATE TABLE IF NOT EXISTS call_conclusions (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  cliente_id  TEXT        NOT NULL,
  name        TEXT        NOT NULL,
  analysis    TEXT        NOT NULL,
  report_ids  TEXT[]      NOT NULL DEFAULT '{}',
  report_names TEXT[]     NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_conclusions_cliente ON call_conclusions (cliente_id, created_at DESC);
