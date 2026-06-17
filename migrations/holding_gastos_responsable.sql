-- Add responsable column to holding_gastos
ALTER TABLE holding_gastos ADD COLUMN IF NOT EXISTS responsable TEXT DEFAULT 'Mau';
