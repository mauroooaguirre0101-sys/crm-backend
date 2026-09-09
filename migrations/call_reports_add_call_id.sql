ALTER TABLE call_reports ADD COLUMN IF NOT EXISTS call_id TEXT;
CREATE INDEX IF NOT EXISTS idx_call_reports_call ON call_reports (call_id);
