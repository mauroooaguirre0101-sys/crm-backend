'use strict';

const { PLATFORMS, MEDIUMS, RESOLVED_BY } = require('../schema');

/**
 * Adapta la respuesta directa de TikTok Ads API al NormalizedAttribution.
 * Implementar cuando se integre TikTok Ads sin pasar por GHL.
 *
 * Referencia de campos: https://ads.tiktok.com/marketing_api/docs
 *
 * @param {Object} tiktokPayload
 * @returns {import('../schema').NormalizedAttribution|null}
 */
function adaptFromTiktok(tiktokPayload) {
  if (!tiktokPayload || typeof tiktokPayload !== 'object') return null;

  return {
    _source:      RESOLVED_BY.PLATFORM_API,
    platform:     PLATFORMS.TIKTOK,
    medium:       MEDIUMS.PAID_SOCIAL,
    campaignId:   tiktokPayload.campaign_id            || null,
    campaignName: tiktokPayload.campaign_name          || null,
    adsetId:      tiktokPayload.adgroup_id             || null,
    adsetName:    tiktokPayload.adgroup_name           || null,
    adId:         tiktokPayload.ad_id                  || null,
    adName:       tiktokPayload.ad_name                || null,
    clickId:      tiktokPayload.ttclid                 || null,
    landingUrl:   tiktokPayload.landing_page_url       || null,
    extra:        null,
  };
}

module.exports = { adaptFromTiktok };
