'use strict';

// ── Valores canónicos ─────────────────────────────────────────────────────────

const PLATFORMS = Object.freeze({
  META:     'meta',
  GOOGLE:   'google',
  TIKTOK:   'tiktok',
  YOUTUBE:  'youtube',
  ORGANIC:  'organic',
  DIRECT:   'direct',
  REFERRAL: 'referral',
  EMAIL:    'email',
  UNKNOWN:  'unknown',
});

const MEDIUMS = Object.freeze({
  PAID_SOCIAL:  'paid_social',
  PAID_SEARCH:  'paid_search',
  PAID_VIDEO:   'paid_video',
  ORGANIC:      'organic',
  DIRECT:       'direct',
  REFERRAL:     'referral',
  EMAIL:        'email',
  UNKNOWN:      'unknown',
});

/**
 * Nivel de completitud de la atribución.
 *
 * full     → plataforma + campaña + ad set + anuncio resueltos
 * campaign → plataforma + campaña resueltos, ad set / anuncio faltantes
 * platform → solo plataforma resuelta (típico de Click to DM, Follow Me)
 * unknown  → ninguna señal confiable disponible
 */
const CONFIDENCE = Object.freeze({
  FULL:     'full',
  CAMPAIGN: 'campaign',
  PLATFORM: 'platform',
  UNKNOWN:  'unknown',
});

/**
 * Fuente que resolvió la atribución (quién aportó el campo `platform`).
 *
 * platform_api → API directa de la plataforma (Meta Lead Forms, Google Ads API)
 * utms         → Parámetros UTM en la URL de la landing page
 * ghl_native   → firstAttributionSource / lastAttributionSource de GHL
 * tags         → Etiquetas del contacto o triggers de workflow en GHL
 * manual       → Ingresado manualmente por un usuario del CRM
 * unknown      → Ninguna fuente pudo resolver
 */
const RESOLVED_BY = Object.freeze({
  PLATFORM_API: 'platform_api',
  UTMS:         'utms',
  GHL_NATIVE:   'ghl_native',
  TAGS:         'tags',
  MANUAL:       'manual',
  UNKNOWN:      'unknown',
});

const VALID_PLATFORMS = new Set(Object.values(PLATFORMS));
const VALID_MEDIUMS   = new Set(Object.values(MEDIUMS));
const VALID_CONFIDENCE = new Set(Object.values(CONFIDENCE));
const VALID_RESOLVED_BY = new Set(Object.values(RESOLVED_BY));

/**
 * @typedef {Object} NormalizedAttribution
 * Contrato entre los adapters y el resolver/parser.
 * Usa nombres de negocio — ningún campo es específico de una plataforma.
 *
 * @property {string}       platform     - Ver PLATFORMS
 * @property {string}       medium       - Ver MEDIUMS
 * @property {string|null}  campaignId
 * @property {string|null}  campaignName
 * @property {string|null}  adsetId      - Ad Set (Meta) / Ad Group (Google, TikTok)
 * @property {string|null}  adsetName
 * @property {string|null}  adId
 * @property {string|null}  adName
 * @property {string|null}  clickId      - fbclid | gclid | ttclid | msclkid
 * @property {string|null}  landingUrl
 * @property {Object|null}  extra        - Campos propios de la plataforma sin hueco en el modelo
 * @property {string}       _source      - Campo interno (RESOLVED_BY.*). Lo usa el resolver,
 *                                         no se persiste en la DB.
 */

/**
 * @typedef {Object} AttributionRecord
 * Forma final que se guarda en JSONB dentro de la tabla leads.
 *
 * @property {string}       platform
 * @property {string}       medium
 * @property {string|null}  campaign_id
 * @property {string|null}  campaign_name
 * @property {string|null}  adset_id
 * @property {string|null}  adset_name
 * @property {string|null}  ad_id
 * @property {string|null}  ad_name
 * @property {string|null}  click_id
 * @property {string|null}  landing_url
 * @property {Object|null}  extra
 * @property {string}       confidence   - Ver CONFIDENCE
 * @property {string}       resolved_by  - Ver RESOLVED_BY
 * @property {string}       resolved_at  - ISO 8601
 */

module.exports = {
  PLATFORMS, MEDIUMS, CONFIDENCE, RESOLVED_BY,
  VALID_PLATFORMS, VALID_MEDIUMS, VALID_CONFIDENCE, VALID_RESOLVED_BY,
};
