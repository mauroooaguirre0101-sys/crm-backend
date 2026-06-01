-- Agrega columna formula JSONB a monthly_objectives
-- Permite métricas custom definidas por AI basadas en instrucciones del admin

ALTER TABLE monthly_objectives
  ADD COLUMN IF NOT EXISTS formula JSONB;
