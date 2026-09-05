-- Tabla para diagnósticos de barberos (formulario público)
CREATE TABLE IF NOT EXISTS diagnosticos_barbero (
  id           uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  nombre       text        NOT NULL,
  celular      text,
  instagram    text,
  comprometido boolean,
  avatar_tipo  text        NOT NULL CHECK (avatar_tipo IN ('dueno', 'futuro', 'segunda')),
  respuestas   jsonb       NOT NULL DEFAULT '{}',
  diagnostico  text,
  created_at   timestamptz DEFAULT now()
);

-- Índices útiles para consultar la base de datos
CREATE INDEX IF NOT EXISTS idx_diagnosticos_avatar ON diagnosticos_barbero (avatar_tipo);
CREATE INDEX IF NOT EXISTS idx_diagnosticos_comprometido ON diagnosticos_barbero (comprometido);
CREATE INDEX IF NOT EXISTS idx_diagnosticos_created ON diagnosticos_barbero (created_at DESC);
