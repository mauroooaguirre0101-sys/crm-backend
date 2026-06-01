-- Apodos de usuarios (visible para todos)
CREATE TABLE IF NOT EXISTS user_nicknames (
  user_email  text        PRIMARY KEY,
  nickname    text        NOT NULL,
  updated_at  timestamptz DEFAULT now()
);

-- Aliases de clientes/negocios (visible para todos los usuarios del sistema)
CREATE TABLE IF NOT EXISTS client_aliases (
  cliente_id  text        PRIMARY KEY,
  alias       text        NOT NULL,
  updated_at  timestamptz DEFAULT now()
);
