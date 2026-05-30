-- Tracking de Seguimiento Post Call: fecha de inicio y notas detalladas
ALTER TABLE calls ADD COLUMN IF NOT EXISTS spc_date timestamptz;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS notas_spc text;
