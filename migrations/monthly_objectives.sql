-- Objetivos mensuales por negocio
-- Cada objetivo tiene un tipo de métrica que se resuelve automáticamente
-- contra los datos del CRM (calls, leads, clientes, ingresos)

CREATE TABLE IF NOT EXISTS monthly_objectives (
  id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  cliente_id  text        NOT NULL,
  mes         int         NOT NULL CHECK (mes BETWEEN 1 AND 12),
  año         int         NOT NULL,
  titulo      text        NOT NULL,
  tipo_metrica text       NOT NULL,
  meta        numeric     NOT NULL CHECK (meta > 0),
  created_by  text,
  created_at  timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_monthly_objectives_cliente_mes_año
  ON monthly_objectives(cliente_id, mes, año);
