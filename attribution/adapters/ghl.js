'use strict';

const { PLATFORMS, MEDIUMS, RESOLVED_BY } = require('../schema');

/**
 * Adapta el firstAttributionSource de GHL al NormalizedAttribution.
 *
 * @param {Object} ghlAttribution - contact.firstAttributionSource de GHL
 * @returns {import('../schema').NormalizedAttribution|null}
 */
function adaptFirstFromGhl(ghlAttribution) {
  return _adapt(ghlAttribution, RESOLVED_BY.GHL_NATIVE);
}

/**
 * Adapta el lastAttributionSource de GHL al NormalizedAttribution.
 * La estructura de campos es idéntica a firstAttributionSource en la API de GHL.
 *
 * @param {Object} ghlAttribution - contact.lastAttributionSource de GHL
 * @returns {import('../schema').NormalizedAttribution|null}
 */
function adaptLastFromGhl(ghlAttribution) {
  return _adapt(ghlAttribution, RESOLVED_BY.GHL_NATIVE);
}

// ── Función interna compartida ────────────────────────────────────────────────

function _adapt(raw, source) {
  if (!raw || typeof raw !== 'object') return null;

  const platform = _inferPlatform(raw);
  const medium   = _inferMedium(raw);

  return {
    // Campo interno usado por el resolver para asignar resolved_by.
    // El parser lo consume y no lo persiste en la DB.
    _source: source,

    platform,
    medium,

    // Jerarquía del ad.
    // GHL provee los IDs nativos cuando el lead viene de Facebook Lead Form.
    // Para Meta Ads → Landing, los nombres llegan via UTMs serializados por GHL.
    campaignId:   _first(raw.campaignId),
    campaignName: _first(raw.campaign, raw.utmCampaign),
    adsetId:      _first(raw.adSetId, raw.adGroupId),
    adsetName:    _first(raw.adSetName, raw.utmMedium),   // GHL serializa el adset name en utmMedium para Meta
    adId:         _first(raw.adId),
    adName:       _first(raw.adName, raw.utmContent),     // GHL serializa el ad name en utmContent
    clickId:      _first(raw.fbclid, raw.gclid, raw.ttclid, raw.msclkid, raw.ctwaCLid),
    landingUrl:   _first(raw.url, raw.referrer),
    extra:        _buildExtra(platform, raw),
  };
}

// ── Inferencia de plataforma y medio ─────────────────────────────────────────
// Lógica basada en los valores que GHL documenta para cada categoría de tráfico.
// sessionSource es la señal más confiable — es la categorización propia de GHL.
// utmSource / medium se usan como fallback cuando sessionSource no es suficiente.

function _inferPlatform(raw) {
  const session = _lower(raw.sessionSource);
  const source  = _lower(raw.utmSource, raw.medium);

  if (session === 'paid social') {
    if (source === 'tiktok')    return PLATFORMS.TIKTOK;
    return PLATFORMS.META; // paid social sin source explícito → Meta en este negocio
  }
  if (session === 'paid search')    return PLATFORMS.GOOGLE;
  if (source  === 'youtube')        return PLATFORMS.YOUTUBE;
  if (session === 'social media')   return PLATFORMS.ORGANIC; // orgánico en redes sociales
  if (session === 'organic search' || source === 'organic') return PLATFORMS.ORGANIC;
  if (session === 'direct traffic') return PLATFORMS.DIRECT;
  if (session === 'referrals')      return PLATFORMS.REFERRAL;
  if (session === 'email')          return PLATFORMS.EMAIL;

  // Fallback desde utmSource cuando sessionSource no es categorizable
  const sourceMap = {
    fb_ad: PLATFORMS.META, facebook: PLATFORMS.META, instagram: PLATFORMS.META,
    google: PLATFORMS.GOOGLE, adwords: PLATFORMS.GOOGLE,
    tiktok: PLATFORMS.TIKTOK,
    youtube: PLATFORMS.YOUTUBE,
  };
  return sourceMap[source] || PLATFORMS.UNKNOWN;
}

function _inferMedium(raw) {
  const session = _lower(raw.sessionSource);

  const mediumMap = {
    'paid social':    MEDIUMS.PAID_SOCIAL,
    'paid search':    MEDIUMS.PAID_SEARCH,
    'social media':   MEDIUMS.ORGANIC,
    'organic search': MEDIUMS.ORGANIC,
    'direct traffic': MEDIUMS.DIRECT,
    'referrals':      MEDIUMS.REFERRAL,
    'email':          MEDIUMS.EMAIL,
  };
  if (mediumMap[session]) return mediumMap[session];

  if (_lower(raw.utmSource) === 'youtube') return MEDIUMS.PAID_VIDEO;

  return MEDIUMS.UNKNOWN;
}

// ── Extra ─────────────────────────────────────────────────────────────────────
// Solo campos genuinamente plataforma-específicos que no tienen hueco en el modelo.

function _buildExtra(platform, raw) {
  const extra = {};

  if (platform === PLATFORMS.GOOGLE) {
    if (raw.utmKeyword)   extra.keyword   = raw.utmKeyword;
    if (raw.utmMatchType) extra.matchtype = raw.utmMatchType;
  }
  // ctwaCLid indica Click to DM/WhatsApp — señal útil para medir ese flow
  if (raw.ctwaCLid) extra.ctwaCLid = raw.ctwaCLid;

  return Object.keys(extra).length > 0 ? extra : null;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function _first(...vals) {
  for (const v of vals) {
    if (v !== null && v !== undefined) {
      const s = String(v).trim();
      if (s) return s;
    }
  }
  return null;
}

function _lower(...vals) {
  for (const v of vals) {
    if (v !== null && v !== undefined) {
      const s = String(v).trim();
      if (s) return s.toLowerCase();
    }
  }
  return '';
}

module.exports = { adaptFirstFromGhl, adaptLastFromGhl };
