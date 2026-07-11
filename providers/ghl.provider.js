'use strict';
const crypto = require('crypto');

const GHL_API     = 'https://services.leadconnectorhq.com';
const GHL_AUTH    = 'https://marketplace.gohighlevel.com';
const GHL_VERSION = '2021-07-28';

// ── HTTP helpers ──────────────────────────────────────────────────────────────

async function _apiCall(method, path, body, accessToken) {
  const opts = {
    method,
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type':  'application/json',
      'Version':       GHL_VERSION,
    },
  };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${GHL_API}${path}`, opts);
  if (res.status === 204) return null;
  const text = await res.text();
  if (!text) {
    if (!res.ok) throw new Error(`GHL ${method} ${path} [${res.status}]: (empty response)`);
    return null;
  }
  let data;
  try { data = JSON.parse(text); } catch (e) {
    if (!res.ok) throw new Error(`GHL ${method} ${path} [${res.status}]: ${text.slice(0, 400)}`);
    return null;
  }
  if (!res.ok) throw new Error(`GHL ${method} ${path} [${res.status}]: ${JSON.stringify(data).slice(0, 400)}`);
  return data;
}

async function _tokenCall(params) {
  if (!process.env.GHL_CLIENT_ID || !process.env.GHL_CLIENT_SECRET)
    throw new Error('GHL_CLIENT_ID / GHL_CLIENT_SECRET not configured');

  const res = await fetch(`${GHL_API}/oauth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:   new URLSearchParams({
      ...params,
      client_id:     process.env.GHL_CLIENT_ID,
      client_secret: process.env.GHL_CLIENT_SECRET,
      user_type:     'Location',   // required by GHL for location-scoped tokens
    }).toString(),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch (e) { data = {}; }
  if (!res.ok) throw new Error(`GHL token [${res.status}]: ${text.slice(0, 400)}`);
  return data;
}

// ── Public API ────────────────────────────────────────────────────────────────

// Build the GHL OAuth authorization URL
function buildOAuthURL(redirectUri, state) {
  const q = new URLSearchParams({
    response_type: 'code',
    redirect_uri:  redirectUri,
    client_id:     process.env.GHL_CLIENT_ID || '',
    // Scopes GHL v2 requires for contacts, calendars and locations
    scope: [
      'contacts.readonly',
      'calendars.readonly',
      'calendars/appointments.readonly',
      'locations.readonly',
    ].join(' '),
    state: typeof state === 'string' ? state : JSON.stringify(state),
  });
  return `${GHL_AUTH}/oauth/chooselocation?${q}`;
}

// Exchange authorization code for tokens
async function exchangeCode(code, redirectUri) {
  return _tokenCall({ grant_type: 'authorization_code', code, redirect_uri: redirectUri });
}

// Refresh an expired access token
async function refreshAccessToken(refreshToken) {
  return _tokenCall({ grant_type: 'refresh_token', refresh_token: refreshToken });
}

// Get location (sub-account) info
async function getLocation(accessToken, locationId) {
  const data = await _apiCall('GET', `/locations/${locationId}`, null, accessToken);
  return data?.location || data;
}

// Get a contact by ID — locationId is implicit from the token scope
async function getContact(accessToken, contactId) {
  const data = await _apiCall('GET', `/contacts/${contactId}`, null, accessToken);
  return data?.contact || data;
}

// Get an appointment by ID
async function getAppointment(accessToken, appointmentId) {
  const data = await _apiCall('GET', `/calendars/events/appointments/${appointmentId}`, null, accessToken);
  return data?.appointment || data?.event || data;
}

// Get all users for a location (used to resolve assignedUserId → name)
async function getLocationUsers(accessToken, locationId) {
  const data = await _apiCall('GET', `/users/search?locationId=${encodeURIComponent(locationId)}&limit=100`, null, accessToken);
  return data?.users || data || [];
}

// Create a webhook subscription for appointment events
// GHL v2: POST /webhooks (no trailing slash), locationId in body
async function createWebhookSubscription(accessToken, locationId, callbackUrl) {
  const data = await _apiCall('POST', '/webhooks', {
    name:       'CRM Appointments Sync',
    url:        callbackUrl,
    events:     ['ContactCreate', 'AppointmentCreate', 'AppointmentUpdate', 'AppointmentDelete', 'AppointmentRescheduled'],
    locationId,
  }, accessToken);
  return data;
}

// Delete a webhook subscription by ID
async function deleteWebhookSubscription(accessToken, webhookId) {
  try {
    await _apiCall('DELETE', `/webhooks/${webhookId}`, null, accessToken);
    return true;
  } catch (err) {
    if (err.message.includes('[404]')) return true;
    throw err;
  }
}

// List webhooks for a location
async function listWebhooks(accessToken, locationId) {
  const data = await _apiCall('GET', `/webhooks?locationId=${encodeURIComponent(locationId)}`, null, accessToken);
  return data?.webhooks || data || [];
}

// Ensure the access token is fresh — refresh if expiring in < 5 minutes
// Returns { accessToken, updated, newRefresh?, newExpires? }
async function ensureFreshToken(conn) {
  if (!conn.token_expires_at) return { accessToken: conn.access_token, updated: false };
  const expiresMs = new Date(conn.token_expires_at).getTime();
  if (Date.now() + 5 * 60 * 1000 < expiresMs) return { accessToken: conn.access_token, updated: false };

  const tokens     = await refreshAccessToken(conn.refresh_token);
  const newExpires = new Date(Date.now() + (tokens.expires_in || 86400) * 1000).toISOString();
  return {
    accessToken: tokens.access_token,
    newRefresh:  tokens.refresh_token || conn.refresh_token,
    newExpires,
    updated:     true,
  };
}

// Generate a secure random webhook token (used as ?t= URL param)
function generateWebhookToken() {
  return crypto.randomBytes(24).toString('hex');
}

// Normalize GHL webhook payload — handles field name variations across GHL versions
// GHL v2 uses camelCase; some older/workflow webhooks use snake_case or nested objects
function normalizeWebhookPayload(body) {
  // Event type — GHL v2: "AppointmentCreate"; v1/workflow: "appointment_created"
  let type = body.type
    || (body.event && _normalizeEventType(body.event))
    || null;

  // Infer event type if ANY GHL workflow signal is present and no explicit type
  // GHL typos "appointmentStatus" as "appoinmentStatus" (missing 't') in calendar object
  let inferred = false;
  if (!type && (body.calendar || body.workflow || body.contact || body.contact_id || body.contactId || body.triggerData)) {
    const rawStatus = (
      body.appointmentStatus
      || body.calendar?.appoinmentStatus
      || body.calendar?.appointmentStatus
      || ''
    ).toLowerCase();
    type = (rawStatus === 'cancelled' || rawStatus === 'cancel') ? 'AppointmentCancelled' : 'AppointmentCreate';
    inferred = true;
  }

  // Appointment ID — top-level OR nested in body.calendar
  // Prefer calendar.appointmentId over calendar.id — calendar.id is the booking-page template ID
  // shared across all appointments in that calendar, NOT a unique per-appointment identifier.
  const appointmentId = body.id
    || body.appointmentId
    || body.appointment_id
    || body.resourceId
    || (body.calendar && (body.calendar.appointmentId || body.calendar.id))
    || null;

  // Contact ID — top-level OR nested in body.contact
  const contactId = body.contactId
    || body.contact_id
    || (body.contact && (body.contact.id || body.contact._id))
    || null;

  // Location ID — top-level, nested body.location, or nested body.payload.location
  const locationId = body.locationId || body.location_id
    || (body.location && (body.location.id || body.location._id))
    || (body.payload && body.payload.location && (body.payload.location.id || body.payload.location._id))
    || null;

  // Embedded objects present in new GHL workflow format
  const embeddedContact  = body.contact  || null;
  const embeddedCalendar = body.calendar || null;

  return { type, inferred, appointmentId, contactId, locationId, embeddedContact, embeddedCalendar, raw: body };
}

function _normalizeEventType(event) {
  const map = {
    'appointment_created':      'AppointmentCreate',
    'appointment_updated':      'AppointmentUpdate',
    'appointment_deleted':      'AppointmentDelete',
    'appointment_cancelled':    'AppointmentCancelled',
    'appointment_rescheduled':  'AppointmentRescheduled',
    'appointment_confirmed':    'AppointmentConfirmed',
    'appointment_no_show':      'AppointmentNoShow',
    'appointment_completed':    'AppointmentCompleted',
  };
  return map[event?.toLowerCase()] || event;
}

// Recursively traverse any object looking for a key that matches instagram patterns.
// Returns { key, value } on first match, or null. Max depth 5 to avoid infinite loops.
function _deepFindIgValue(obj, depth) {
  if (!obj || typeof obj !== 'object' || (depth || 0) > 5) return null;
  for (const [key, val] of Object.entries(obj)) {
    const kl = String(key).toLowerCase();
    const isIgKey = kl.includes('instagram') || kl.includes('insta')
      || kl === 'ig' || kl.startsWith('ig_')
      || kl.includes('@') || kl.includes('usuario');
    if (isIgKey && val) {
      const igStr = typeof val === 'string' ? val.trim()
        : Array.isArray(val) ? val.filter(v => typeof v === 'string').join('').trim()
        : '';
      if (igStr) return { key, value: igStr };
    }
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      const found = _deepFindIgValue(val, (depth || 0) + 1);
      if (found) return found;
    }
  }
  return null;
}

// Extract instagram handle — searches contact fields, then recursively scans entire rawPayload
function extractInstagram(contact, rawPayload) {
  if (!contact)    contact    = {};
  if (!rawPayload) rawPayload = {};
  const _clean = (v) => String(v).replace(/^@/, '').replace(/\s+/g, '').toLowerCase().trim();

  // 1. contact.customField / customFields array (OAuth / GHL API format)
  const customFields = contact.customField || contact.customFields || [];
  for (const cf of customFields) {
    const key = String(cf.id || cf.key || cf.name || cf.fieldKey || '').toLowerCase();
    const val = String(cf.value || '').trim();
    const isIgKey = key.includes('instagram') || key.includes('insta') || key === 'ig'
      || key.startsWith('ig_') || key.includes('@') || key.includes('usuario');
    if (val && isIgKey) return _clean(val);
  }

  // 2. Deep recursive scan of rawPayload (catches customData, triggerData, nested objects)
  const foundInPayload = _deepFindIgValue(rawPayload);
  if (foundInPayload) {
    console.log(`[GHL Parser] candidate instagram key="${foundInPayload.key}" value="${foundInPayload.value}"`);
    return _clean(foundInPayload.value);
  }

  // 3. Deep recursive scan of contact object
  const foundInContact = _deepFindIgValue(contact);
  if (foundInContact) {
    console.log(`[GHL Parser] candidate instagram key="${foundInContact.key}" value="${foundInContact.value}"`);
    return _clean(foundInContact.value);
  }

  // 4. Fallback: search notes for @handle pattern
  const notes = String(contact.notes || '');
  const match = notes.match(/@([\w.]+)/);
  if (match) return match[1].toLowerCase();

  // 5. rawPayload.name or contact.name if value starts with '@'
  for (const candidate of [rawPayload.name, contact.name]) {
    const s = String(candidate || '').trim();
    if (s.startsWith('@')) return _clean(s);
  }

  // 6. lastName field when it looks like a social handle — covers GHL forms that map the
  // instagram question to the built-in lastName contact field.
  // Heuristic: value must be ASCII-only (no accented chars) + contain _ or . (never in real surnames)
  // Real Spanish/Latin surnames almost always have accented chars, spaces, or hyphens.
  const firstNamePresent = !!(rawPayload.firstName || rawPayload.first_name
    || contact.firstName || contact.first_name);
  if (firstNamePresent) {
    const lastNameCandidates = [
      contact.lastName, contact.last_name,
      rawPayload.lastName, rawPayload.last_name,
    ];
    for (const candidate of lastNameCandidates) {
      const s = String(candidate || '').trim();
      if (!s) continue;
      if (/^@?[a-zA-Z0-9_.]+$/.test(s) && /[_.]/.test(s)) {
        console.log(`[GHL Parser] instagram fallback from lastName="${s}"`);
        return _clean(s);
      }
    }
  }

  return '';
}

// Metadata keys to exclude — GHL system fields that are NOT form answers.
// Match is done on the ORIGINAL key (lowercased only), NOT after stripping punctuation,
// so Spanish question strings like "¿Cuál es tu presupuesto?" are never accidentally excluded.
const _QUAL_EXCLUDED_KEYS = new Set([
  // contact identity
  'contact_id', 'contactid', 'first_name', 'firstname', 'last_name', 'lastname',
  'full_name', 'fullname', 'name', 'email', 'phone', 'tags',
  'country', 'timezone', 'date_created', 'datecreated',
  'contact_source', 'contactsource', 'full_address', 'fulladdress',
  'contact_type', 'contacttype',
  // nested GHL objects (values are objects/arrays — already skipped by strVal check, but listed for clarity)
  'location', 'user', 'calendar', 'workflow', 'triggerdata', 'customdata',
  'attributionsource', 'attribution_source', 'contact',
  // technical ids & timestamps
  'id', '_id', 'locationid', 'location_id', 'calendarid', 'calendar_id',
  'workflowid', 'workflow_id', 'triggerid', 'trigger_id',
  'appointmentid', 'appointment_id',
  'createdat', 'created_at', 'updatedat', 'updated_at', 'timestamp',
  // event routing
  'type', 'event', 'source',
]);
// Only test these patterns on the LOWERCASED original key — no stripping applied.
const _QUAL_EXCLUDED_PATTERNS = [
  /^utm_/i,
  /^attribution/i,
  /fbclid/i,
  /gclid/i,
  /\bwebhook\b/i,
  /\btracking\b/i,
  /\bbrowser\b/i,
  /\buser.?agent\b/i,
  /^ip$/i,
  /\bplatform\b/i,
];

// Extract human-readable qualification answers from a GHL workflow payload.
// GHL sends form answers as TOP-LEVEL keys in the body (long Spanish question strings).
// Also checks customData / triggerData as fallback.
function extractQualificationAnswers(rawPayload) {
  if (!rawPayload) return {};
  const answers = {};

  // Exclusion check uses LOWERCASED original key — no punctuation stripping so Spanish
  // question strings with ¿ ? spaces are never accidentally matched to a system key.
  const _exclude = (key) => {
    const kl = key.toLowerCase();
    return _QUAL_EXCLUDED_KEYS.has(kl) || _QUAL_EXCLUDED_PATTERNS.some(p => p.test(kl));
  };

  const _toStr = (val) => {
    if (typeof val === 'string') return val.trim();
    if (Array.isArray(val)) return val.filter(v => typeof v === 'string').join(', ').trim();
    return '';
  };

  // 1. Scan TOP-LEVEL body keys — this is where GHL workflow sends form answers
  for (const [key, val] of Object.entries(rawPayload)) {
    const strVal = _toStr(val);
    if (!strVal) continue; // skip objects, empty arrays, empty strings
    if (_exclude(key)) continue;
    console.log(`[GHL Parser] qualification candidate key="${key}"`);
    answers[key] = strVal;
  }

  // 2. Also scan customData / triggerData (some GHL setups may use these)
  for (const nested of [rawPayload.customData, rawPayload.triggerData]) {
    if (!nested || typeof nested !== 'object') continue;
    for (const [key, val] of Object.entries(nested)) {
      const strVal = _toStr(val);
      if (!strVal || _exclude(key) || answers[key]) continue;
      console.log(`[GHL Parser] qualification candidate key="${key}" (nested)`);
      answers[key] = strVal;
    }
  }

  return answers;
}

// List appointments/events within a time range for a location
async function listAppointments(accessToken, locationId, startTime, endTime) {
  const params = new URLSearchParams({ locationId });
  if (startTime) params.set('startTime', startTime);
  if (endTime)   params.set('endTime', endTime);
  const data = await _apiCall('GET', `/calendars/events?${params}`, null, accessToken);
  return data?.appointments || data?.events || [];
}

// Map GHL appointment status to CRM call estado
function mapAppointmentStatus(ghlStatus) {
  switch ((ghlStatus || '').toLowerCase()) {
    case 'cancelled':
    case 'cancel':        return 'Cancelada';
    case 'showed':
    case 'show':
    case 'completed':     return 'Completada';
    case 'noshow':
    case 'no_show':
    case 'no-show':       return 'No asistió';
    case 'rescheduled':   return 'Re agenda';
    case 'booked':
    case 'confirmed':
    case 'new':           return 'Pendiente';
    default:              return 'Pendiente';
  }
}

// Derive CRM estado from GHL event type (for events without an appointmentStatus)
function mapEventTypeToEstado(eventType) {
  switch (eventType) {
    case 'AppointmentDelete':
    case 'AppointmentCancelled':  return 'Cancelada';
    case 'AppointmentNoShow':     return 'No asistió';
    case 'AppointmentCompleted':  return 'Completada';
    case 'AppointmentRescheduled':return 'Re agenda';
    case 'AppointmentConfirmed':  return 'Pendiente';
    default:                      return null; // use appointmentStatus
  }
}

module.exports = {
  buildOAuthURL,
  exchangeCode,
  refreshAccessToken,
  getLocation,
  getContact,
  getAppointment,
  getLocationUsers,
  listAppointments,
  createWebhookSubscription,
  deleteWebhookSubscription,
  listWebhooks,
  ensureFreshToken,
  generateWebhookToken,
  normalizeWebhookPayload,
  extractInstagram,
  extractQualificationAnswers,
  mapAppointmentStatus,
  mapEventTypeToEstado,
};
