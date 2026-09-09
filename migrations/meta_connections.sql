-- Meta / Instagram Business OAuth connections
-- One row per negocio. Stores the long-lived user token + selected Page + IG account.
-- Run once in Supabase SQL editor.

CREATE TABLE IF NOT EXISTS meta_connections (
  id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  negocio_id            TEXT        NOT NULL UNIQUE,

  -- Facebook user (who authorized the app)
  fb_user_id            TEXT,
  fb_user_name          TEXT,

  -- Facebook Page selected by the user
  fb_page_id            TEXT,
  fb_page_name          TEXT,
  fb_page_access_token  TEXT,   -- Never expires (derived from long-lived user token)

  -- Instagram Business Account linked to the Page
  ig_account_id         TEXT,
  ig_username           TEXT,

  -- Long-lived user token (60 days; auto-refreshed when < 15 days remain)
  long_lived_token      TEXT,
  token_expires_at      TIMESTAMPTZ,

  -- Comma-separated list of granted scopes (for debugging / future checks)
  scopes                TEXT,

  connected_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_meta_connections_negocio ON meta_connections (negocio_id);
