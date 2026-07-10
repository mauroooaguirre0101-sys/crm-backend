'use strict';

const {
  VALID_PLATFORMS, VALID_MEDIUMS, VALID_RESOLVED_BY,
  CONFIDENCE, RESOLVED_BY,
} = require('./schema');

/**
 * Transforma un NormalizedAttribution en un AttributionRecord listo para la DB.
 *
 * Responsabilidades:
 *   - Coerción de tipos y saneamiento
 *   - Validar valores contra los sets canónicos
 *   - Calcular confidence basado en qué campos se pudieron resolver
 *   - Mapear claves del contrato a los nombres de columna JSONB
 *   - Agregar metadata de resolución (resolved_by, resolved_at)
 *
 * No responsabilidades:
 *   - Saber cómo se llaman los campos en GHL, Meta, Google, etc.
 *   - Elegir entre múltiples fuentes (eso es el resolver)
 *   - Ninguna lógica de negocio más allá de la transformación
 *
 * @param {import('./schema').NormalizedAttribution|null} input
 * @returns {import('./schema').AttributionRecord|null}
 */
function parseAttribution(input) {
  if (!input || typeof input !== 'object') return null;

  const platform = _canon(_str(input.platform), VALID_PLATFORMS, 'unknown');
  const medium   = _canon(_str(input.medium),   VALID_MEDIUMS,   'unknown');

  const record = {
    platform,
    medium,
    campaign_id:   _str(input.campaignId),
    campaign_name: _str(input.campaignName),
    adset_id:      _str(input.adsetId),
    adset_name:    _str(input.adsetName),
    ad_id:         _str(input.adId),
    ad_name:       _str(input.adName),
    click_id:      _str(input.clickId),
    landing_url:   _str(input.landingUrl),
    extra:         _obj(input.extra),
    confidence:    _calculateConfidence(platform, input),
    resolved_by:   _canon(_str(input._source), VALID_RESOLVED_BY, RESOLVED_BY.UNKNOWN),
    resolved_at:   new Date().toISOString(),
  };

  return record;
}

// ── Cálculo de confianza ──────────────────────────────────────────────────────
// Puramente mecánico: cuántos niveles de la jerarquía de ad se resolvieron.

function _calculateConfidence(platform, input) {
  if (platform === 'unknown') return CONFIDENCE.UNKNOWN;

  const hasCampaign = !!(_str(input.campaignId) || _str(input.campaignName));
  const hasAdset    = !!(_str(input.adsetId)    || _str(input.adsetName));
  const hasAd       = !!(_str(input.adId)       || _str(input.adName));

  // Para orgánico/directo la jerarquía de ad no aplica → full por definición
  if (['organic', 'direct', 'referral', 'email'].includes(platform)) {
    return CONFIDENCE.FULL;
  }

  if (hasCampaign && hasAdset && hasAd) return CONFIDENCE.FULL;
  if (hasCampaign)                      return CONFIDENCE.CAMPAIGN;
  return CONFIDENCE.PLATFORM;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function _str(val) {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s || null;
}

function _obj(val) {
  if (!val || typeof val !== 'object' || Array.isArray(val)) return null;
  return Object.keys(val).length > 0 ? val : null;
}

function _canon(val, validSet, fallback) {
  return validSet.has(val) ? val : fallback;
}

module.exports = { parseAttribution };
