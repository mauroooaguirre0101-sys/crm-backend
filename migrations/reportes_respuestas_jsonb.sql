-- Store full dynamic form responses as JSON alongside legacy fixed columns.
-- Legacy columns (estado, situacion, etc.) stay populated for backward compat.
ALTER TABLE reportes_semanales ADD COLUMN IF NOT EXISTS respuestas JSONB;
