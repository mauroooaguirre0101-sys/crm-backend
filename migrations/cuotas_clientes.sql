-- Tabla para persistir cuotas de clientes (antes solo en localStorage del frontend)
CREATE TABLE IF NOT EXISTS cuotas_clientes (
  id           text PRIMARY KEY DEFAULT (gen_random_uuid()::text),
  cliente_id   text NOT NULL,
  ref_cliente_id text,
  cliente_nombre text,
  cliente_ig   text,
  numero       integer NOT NULL DEFAULT 2,
  fecha        date,
  monto        numeric(12,2) DEFAULT 0,
  pagado       boolean DEFAULT false,
  cash_collected numeric(12,2) DEFAULT 0,
  created_at   timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cuotas_clientes_tenant ON cuotas_clientes(cliente_id);
CREATE INDEX IF NOT EXISTS idx_cuotas_clientes_ref    ON cuotas_clientes(ref_cliente_id);
