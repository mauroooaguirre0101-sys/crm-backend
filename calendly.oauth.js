'use strict';
const crypto = require('crypto');

const CALENDLY_API  = 'https://api.calendly.com';
const CALENDLY_AUTH = 'https://auth.calendly.com';

// ── HTTP helpers ─────────────────────────────────────────────────────────────

async function _apiCall(method, path, body, accessToken) {
  const opts = {
    method,
    headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${CALENDLY_API}${path}`, opts);
  if (res.status === 204) return null;
  const data = await res.json();
  if (!res.ok) throw new Error(`Calendly ${method} ${path} [${res.status}]: ${JSON.stringify(data).slice(0, 300)}`);
  return data;
}

async function _tokenCall(params) {
  if (!process.env.CALENDLY_CLIENT_ID || !process.env.CALENDLY_CLIENT_SECRET)
    throw new Error('CALENDLY_CLIENT_ID / CALENDLY_CLIENT_SECRET not configured');

  const creds = Buffer.from(`${process.env.CALENDLY_CLIENT_ID}:${process.env.CALENDLY_CLIENT_SECRET}`).toString('base64');
  const res   = await fetch(`${CALENDLY_AUTH}/oauth/token`, {
    method: 'POST',
    headers: { 'Authorization': `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body:   new URLSearchParams(params).toString(),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Calendly token [${res.status}]: ${JSON.stringify(data).slice(0, 300)}`);
  return data;
}

// ── Public API ────────────────────────────────────────────────────────────────

// Build the Calendly OAuth authorization URL
function buildOAuthURL(redirectUri, state) {
  const q = new URLSearchParams({
    client_id:     process.env.CALENDLY_CLIENT_ID || '',
    response_type: 'code',
    redirect_uri:  redirectUri,
    state:         typeof state === 'string' ? state : JSON.stringify(state),
  });
  return `${CALENDLY_AUTH}/oauth/authorize?${q}`;
}

// Exchange authorization code for tokens
async function exchangeCode(code, redirectUri) {
  return _tokenCall({ grant_type: 'authorization_code', code, redirect_uri: redirectUri });
}

// Refresh an expired access token
async function refreshAccessToken(refreshToken) {
  return _tokenCall({ grant_type: 'refresh_token', refresh_token: refreshToken });
}

// Get current user info from Calendly
async function getCurrentUser(accessToken) {
  const data = await _apiCall('GET', '/users/me', null, accessToken);
  return data?.resource || data;
}

// Create a user-scoped webhook subscription
async function createWebhookSubscription(accessToken, orgUri, userUri, callbackUrl) {
  const data = await _apiCall('POST', '/webhook_subscriptions', {
    url:          callbackUrl,
    events:       ['invitee.created', 'invitee.canceled', 'invitee.rescheduled'],
    organization: orgUri,
    user:         userUri,
    scope:        'user',
  }, accessToken);
  return data?.resource || data;
}

// Delete a webhook subscription by its Calendly URI
async function deleteWebhookSubscription(accessToken, webhookUri) {
  const uuid = webhookUri.split('/').pop();
  try {
    await _apiCall('DELETE', `/webhook_subscriptions/${uuid}`, null, accessToken);
    return true;
  } catch (err) {
    // 404 = already deleted, treat as success
    if (err.message.includes('[404]')) return true;
    throw err;
  }
}

// Ensure the access token is fresh — refresh if expiring in < 5 minutes
// Returns { accessToken, updated, newRefresh?, newExpires? }
async function ensureFreshToken(conn) {
  if (!conn.token_expires_at) return { accessToken: conn.access_token, updated: false };
  const expiresMs = new Date(conn.token_expires_at).getTime();
  if (Date.now() + 5 * 60 * 1000 < expiresMs) return { accessToken: conn.access_token, updated: false };

  const tokens    = await refreshAccessToken(conn.refresh_token);
  const newExpires = new Date(Date.now() + (tokens.expires_in || 7200) * 1000).toISOString();
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

module.exports = {
  buildOAuthURL,
  exchangeCode,
  refreshAccessToken,
  getCurrentUser,
  createWebhookSubscription,
  deleteWebhookSubscription,
  ensureFreshToken,
  generateWebhookToken,
};
