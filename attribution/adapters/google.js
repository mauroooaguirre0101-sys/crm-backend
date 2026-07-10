'use strict';

const { PLATFORMS, MEDIUMS, RESOLVED_BY } = require('../schema');

/**
 * Adapta la respuesta directa de Google Ads API al NormalizedAttribution.
 * Implementar cuando se integre Google Ads sin pasar por GHL.
 *
 * Referencia de campos: https://developers.google.com/google-ads/api/fields/v17/click_view
 *
 * @param {Object} googlePayload
 * @returns {import('../schema').NormalizedAttribution|null}
 */
function adaptFromGoogle(googlePayload) {
  if (!googlePayload || typeof googlePayload !== 'object') return null;

  return {
    _source:      RESOLVED_BY.PLATFORM_API,
    platform:     PLATFORMS.GOOGLE,
    medium:       MEDIUMS.PAID_SEARCH,
    campaignId:   googlePayload.campaign?.id           || null,
    campaignName: googlePayload.campaign?.name         || null,
    adsetId:      googlePayload.adGroup?.id            || null,
    adsetName:    googlePayload.adGroup?.name          || null,
    adId:         googlePayload.ad?.id                 || null,
    adName:       googlePayload.ad?.name               || null,
    clickId:      googlePayload.gclid                  || null,
    landingUrl:   googlePayload.landingPage            || null,
    extra: _buildExtra(googlePayload),
  };
}

function _buildExtra(raw) {
  const extra = {};
  if (raw.keyword?.text)    extra.keyword   = raw.keyword.text;
  if (raw.keyword?.matchType) extra.matchtype = raw.keyword.matchType;
  return Object.keys(extra).length > 0 ? extra : null;
}

module.exports = { adaptFromGoogle };
