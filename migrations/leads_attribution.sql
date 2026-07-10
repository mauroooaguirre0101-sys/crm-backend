-- Atribución de leads: first-touch y last-touch
--
-- Se usa JSONB en vez de columnas planas por dos razones:
--   1. El modelo de atribución puede crecer (nuevos campos de plataforma) sin migraciones.
--   2. Cada touch es un AttributionRecord cohesivo — no 13 columnas sueltas por touch.
--
-- Schema interno de cada columna JSONB:
--   platform      TEXT   — 'meta' | 'google' | 'tiktok' | 'youtube' | 'organic' | ...
--   medium        TEXT   — 'paid_social' | 'paid_search' | 'paid_video' | 'organic' | ...
--   campaign_id   TEXT
--   campaign_name TEXT
--   adset_id      TEXT   — Ad Set (Meta) / Ad Group (Google, TikTok)
--   adset_name    TEXT
--   ad_id         TEXT
--   ad_name       TEXT
--   click_id      TEXT   — fbclid | gclid | ttclid | msclkid
--   landing_url   TEXT
--   extra         JSONB  — campos plataforma-específicos (keyword, matchtype, placement…)
--   confidence    TEXT   — 'full' | 'campaign' | 'platform' | 'unknown'
--   resolved_by   TEXT   — 'platform_api' | 'utms' | 'ghl_native' | 'tags' | 'manual' | 'unknown'
--   resolved_at   TEXT   — ISO 8601

ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS attr_first_touch JSONB,
  ADD COLUMN IF NOT EXISTS attr_last_touch  JSONB;

-- Índices de expresión sobre los campos más consultados.
-- Permiten filtrar y agrupar por plataforma, campaña y anuncio sin full-scan.
CREATE INDEX IF NOT EXISTS idx_leads_attr_ft_platform   ON leads (cliente_id, (attr_first_touch->>'platform'));
CREATE INDEX IF NOT EXISTS idx_leads_attr_ft_campaign   ON leads (cliente_id, (attr_first_touch->>'campaign_id'));
CREATE INDEX IF NOT EXISTS idx_leads_attr_ft_adset      ON leads (cliente_id, (attr_first_touch->>'adset_id'));
CREATE INDEX IF NOT EXISTS idx_leads_attr_ft_ad         ON leads (cliente_id, (attr_first_touch->>'ad_id'));
CREATE INDEX IF NOT EXISTS idx_leads_attr_ft_confidence ON leads (cliente_id, (attr_first_touch->>'confidence'));

CREATE INDEX IF NOT EXISTS idx_leads_attr_lt_platform   ON leads (cliente_id, (attr_last_touch->>'platform'));
CREATE INDEX IF NOT EXISTS idx_leads_attr_lt_campaign   ON leads (cliente_id, (attr_last_touch->>'campaign_id'));
