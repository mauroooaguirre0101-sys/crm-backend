'use strict';

const { parseAttribution }              = require('./parser');
const { resolveAttribution }            = require('./resolver');
const { PLATFORMS, MEDIUMS, CONFIDENCE, RESOLVED_BY,
        VALID_PLATFORMS, VALID_MEDIUMS } = require('./schema');
const { adaptFirstFromGhl,
        adaptLastFromGhl }              = require('./adapters/ghl');
const { adaptFromGoogle }               = require('./adapters/google');
const { adaptFromTiktok }               = require('./adapters/tiktok');

module.exports = {
  // Capas principales
  resolveAttribution,   // entry point recomendado — recibe sources, devuelve {firstTouch, lastTouch}
  parseAttribution,     // usado internamente por el resolver; disponible para tests

  // Adapters — uno por fuente de datos
  adaptFirstFromGhl,
  adaptLastFromGhl,
  adaptFromGoogle,
  adaptFromTiktok,

  // Constantes
  PLATFORMS, MEDIUMS, CONFIDENCE, RESOLVED_BY,
  VALID_PLATFORMS, VALID_MEDIUMS,
};
