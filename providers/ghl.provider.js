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
  const data = await res.json();
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
  const data = await res.json();
  if (!res.ok) throw new Error(`GHL token [${res.status}]: ${JSON.stringify(data).slice(0, 400)}`);
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
  return data?.appointment || data;
}

// Create a webhook subscription for appointment events
// GHL v2: POST /webhooks (no trailing slash), locationId in body
async function createWebhookSubscription(accessToken, locationId, callbackUrl) {
  const data = await _apiCall('POST', '/webhooks', {
    name:       'CRM Appointments Sync',
    url:        callbackUrl,
    events:     ['AppointmentCreate', 'AppointmentUpdate', 'AppointmentDelete'],
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
  const type = body.type
    || (body.event && _normalizeEventType(body.event))
    || null;

  // Appointment ID — multiple possible locations
  const appointmentId = body.id
    || body.appointmentId
    || body.appointment_id
    || body.resourceId
    || null;

  // Contact ID — multiple possible locations
  const contactId = body.contactId
    || body.contact_id
    || (body.contact && (body.contact.id || body.contact._id))
    || null;

  const locationId = body.locationId || body.location_id || null;

  return { type, appointmentId, contactId, locationId, raw: body };
}

function _normalizeEventType(event) {
  const map = {
    'appointment_created': 'AppointmentCreate',
    'appointment_updated': 'AppointmentUpdate',
    'appointment_deleted': 'AppointmentDelete',
  };
  return map[event?.toLowerCase()] || event;
}

// Extract instagram handle from a GHL contact's custom fields or notes
function extractInstagram(contact) {
  if (!contact) return '';
  const customFields = contact.customField || contact.customFields || [];
  for (const cf of customFields) {
    const key = String(cf.id || cf.key || cf.name || cf.fieldKey || '').toLowerCase();
    const val = String(cf.value || '').trim();
    if (val && (key.includes('instagram') || key === 'ig' || key.startsWith('ig_'))) {
      return val.replace(/^@/, '').replace(/\s+/g, '').toLowerCase();
    }
  }
  // Fallback: search notes for @handle pattern
  const notes = String(contact.notes || '');
  const match = notes.match(/@([\w.]+)/);
  if (match) return match[1].toLowerCase();
  return '';
}

// Map GHL appointment status to CRM call estado
function mapAppointmentStatus(ghlStatus) {
  switch ((ghlStatus || '').toLowerCase()) {
    case 'cancelled': case 'cancel': return 'Cancelada';
    case 'showed':    case 'show':   return 'Completada';
    case 'noshow':    case 'no_show':return 'No asistió';
    default:                         return 'Pendiente';
  }
}

module.exports = {
  buildOAuthURL,
  exchangeCode,
  refreshAccessToken,
  getLocation,
  getContact,
  getAppointment,
  createWebhookSubscription,
  deleteWebhookSubscription,
  listWebhooks,
  ensureFreshToken,
  generateWebhookToken,
  normalizeWebhookPayload,
  extractInstagram,
  mapAppointmentStatus,
};
