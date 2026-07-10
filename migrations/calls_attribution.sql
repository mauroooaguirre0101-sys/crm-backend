-- Atribución en calls — first-touch y last-touch
--
-- Va en calls (no en leads) porque es donde GHL deposita el contacto
-- con su firstAttributionSource / lastAttributionSource.
-- Los leads solo reciben una actualización de estado (Agendado).
--
-- Retrocompatible: ambas columnas son nullable.
-- El resolver las escribe solo cuando tiene datos — no bloquea la creación de la call.
--
-- Schema interno de cada columna (ver attribution/schema.js para valores canónicos):
--   platform, medium, campaign_id, campaign_name, adset_id, adset_name,
--   ad_id, ad_name, click_id, landing_url, extra (JSONB),
--   confidence ('full'|'campaign'|'platform'|'unknown'),
--   resolved_by ('platform_api'|'utms'|'ghl_native'|'tags'|'manual'|'unknown'),
--   resolved_at (ISO 8601)

ALTER TABLE calls
  ADD COLUMN IF NOT EXISTS attr_first_touch JSONB,
  ADD COLUMN IF NOT EXISTS attr_last_touch  JSONB;

-- Índices de expresión para las queries de atribución más comunes
CREATE INDEX IF NOT EXISTS idx_calls_attr_ft_platform   ON calls (cliente_id, (attr_first_touch->>'platform'));
CREATE INDEX IF NOT EXISTS idx_calls_attr_ft_campaign   ON calls (cliente_id, (attr_first_touch->>'campaign_id'));
CREATE INDEX IF NOT EXISTS idx_calls_attr_ft_confidence ON calls (cliente_id, (attr_first_touch->>'confidence'));
CREATE INDEX IF NOT EXISTS idx_calls_attr_lt_platform   ON calls (cliente_id, (attr_last_touch->>'platform'));
