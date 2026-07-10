'use strict';

const { parseAttribution } = require('./parser');
const { RESOLVED_BY }      = require('./schema');

// Orden en que se intentan los campos al hacer gap-filling.
// La plataforma y el medio primero (más fundamentales), luego la jerarquía del ad.
const FILL_ORDER = [
  'platform', 'medium',
  'campaignId', 'campaignName',
  'adsetId', 'adsetName',
  'adId', 'adName',
  'clickId', 'landingUrl',
];

/**
 * Resuelve la atribución de first-touch y last-touch a partir de múltiples fuentes.
 *
 * Estrategia: priority-based gap-filling.
 * Las fuentes se evalúan en orden de prioridad. Para cada campo se toma el primer
 * valor no nulo disponible. Esto permite que una fuente de alta prioridad aporte
 * el campaign_id mientras una de menor prioridad completa el ad_name si la primera
 * no lo tenía.
 *
 * Responsabilidades:
 *   - Decidir qué fuente gana para cada campo
 *   - Combinar fuentes para first-touch y last-touch por separado
 *   - Delegar la transformación final a parseAttribution()
 *
 * No responsabilidades:
 *   - Saber cómo se llaman los campos en GHL o cualquier plataforma
 *   - Parsear formatos de entrada (eso es cada adapter)
 *   - Persistir datos
 *
 * @param {Object} sources
 * @param {import('./schema').NormalizedAttribution|null} sources.platformNative
 *   Datos directos de la API de la plataforma (Meta Lead Forms, Google Ads API).
 *   Prioridad más alta — tiene IDs nativos, sin dependencia de UTMs.
 *
 * @param {import('./schema').NormalizedAttribution|null} sources.utms
 *   Parámetros UTM extraídos de la URL de la landing page.
 *   Segunda prioridad — requiere que el lead haya hecho click en un enlace con UTMs.
 *
 * @param {import('./schema').NormalizedAttribution|null} sources.ghlFirst
 *   firstAttributionSource de GHL — primer contacto registrado.
 *   Cubre flows sin URL (Click to DM, Follow Me) aunque con menos detalle.
 *
 * @param {import('./schema').NormalizedAttribution|null} sources.ghlLast
 *   lastAttributionSource de GHL — interacción más reciente antes de convertir.
 *   Se usa para last-touch. Si es null, last-touch = first-touch.
 *
 * @returns {{ firstTouch: AttributionRecord, lastTouch: AttributionRecord }}
 */
function resolveAttribution({ platformNative = null, utms = null, ghlFirst = null, ghlLast = null } = {}) {
  // Orden de prioridad para first-touch
  const firstSources = [platformNative, utms, ghlFirst].filter(Boolean);

  // Orden de prioridad para last-touch.
  // Si no hay señales de last-touch, last == first (comportamiento esperado
  // cuando el lead convierte en la misma sesión que entra).
  const lastSources = [platformNative, utms, ghlLast].filter(Boolean);

  const firstNorm = _mergeByPriority(firstSources);
  const lastNorm  = lastSources.length > 0
    ? _mergeByPriority(lastSources)
    : firstNorm;

  return {
    firstTouch: parseAttribution(firstNorm),
    lastTouch:  parseAttribution(lastNorm),
  };
}

// ── Gap-filling ───────────────────────────────────────────────────────────────

/**
 * Recorre las fuentes en orden de prioridad y construye un objeto NormalizedAttribution
 * combinado: para cada campo toma el primer valor no nulo disponible.
 *
 * El campo _source del objeto resultante identifica la fuente que aportó `platform`,
 * que es el campo más fundamental. Ese valor se convierte en `resolved_by` en el
 * AttributionRecord final.
 */
function _mergeByPriority(sources) {
  if (!sources || sources.length === 0) return null;

  const merged  = {};
  let resolvedBy = RESOLVED_BY.UNKNOWN;
  let platformSet = false;

  for (const field of FILL_ORDER) {
    for (const source of sources) {
      const val = _nonEmpty(source[field]);
      if (val !== null) {
        merged[field] = val;
        if (!platformSet && field === 'platform') {
          resolvedBy = source._source || RESOLVED_BY.UNKNOWN;
          platformSet = true;
        }
        break; // campo cubierto — siguiente campo
      }
    }
  }

  // Extra: tomar del primer source que lo tenga (no merge de objetos, para evitar
  // mezclar datos de plataformas distintas en un mismo extra)
  const extraSource = sources.find(s => s.extra && typeof s.extra === 'object' && Object.keys(s.extra).length > 0);
  merged.extra   = extraSource?.extra || null;
  merged._source = resolvedBy;

  return merged;
}

function _nonEmpty(val) {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s || null;
}

module.exports = { resolveAttribution };
