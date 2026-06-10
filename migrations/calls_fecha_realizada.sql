-- Tracks when a call actually took place (auto-set by backend on first status change
-- away from Pendiente/Re agenda). NULL = call hasn't happened yet.
ALTER TABLE calls ADD COLUMN IF NOT EXISTS fecha_realizada TIMESTAMPTZ;
