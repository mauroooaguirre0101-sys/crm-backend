'use strict';

const GRAPH = 'https://graph.facebook.com/v21.0';

// ── HTTP helper ───────────────────────────────────────────────────────────────

async function graphGet(path, token, params = {}) {
  const q = new URLSearchParams({ access_token: token, ...params });
  const res = await fetch(`${GRAPH}${path}?${q}`);
  const data = await res.json();
  if (!res.ok || data.error) {
    const msg = data.error?.message || `HTTP ${res.status}`;
    const code = data.error?.code || res.status;
    throw new Error(`Graph API ${path} [${code}]: ${msg}`);
  }
  return data;
}

// ── OAuth URL ─────────────────────────────────────────────────────────────────

const SCOPES = [
  'public_profile',
  'pages_show_list',
  'pages_read_engagement',
  'instagram_basic',
  'instagram_manage_insights',
  'read_insights',
].join(',');

function buildOAuthURL(redirectUri, state) {
  if (!process.env.META_APP_ID) throw new Error('META_APP_ID not configured');
  const q = new URLSearchParams({
    client_id:     process.env.META_APP_ID,
    redirect_uri:  redirectUri,
    response_type: 'code',
    scope:         SCOPES,
    state:         typeof state === 'string' ? state : JSON.stringify(state),
  });
  return `https://www.facebook.com/v21.0/dialog/oauth?${q}`;
}

// ── Token exchange ────────────────────────────────────────────────────────────

// Exchange short-lived code for a short-lived user access token
async function exchangeCode(code, redirectUri) {
  if (!process.env.META_APP_ID || !process.env.META_APP_SECRET)
    throw new Error('META_APP_ID / META_APP_SECRET not configured');

  const q = new URLSearchParams({
    client_id:     process.env.META_APP_ID,
    client_secret: process.env.META_APP_SECRET,
    redirect_uri:  redirectUri,
    code,
  });
  const res = await fetch(`${GRAPH}/oauth/access_token?${q}`);
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(`Token exchange: ${data.error?.message || res.status}`);
  return data; // { access_token, token_type, expires_in? }
}

// Exchange short-lived token for a long-lived token (60 days)
async function getLongLivedToken(shortToken) {
  if (!process.env.META_APP_ID || !process.env.META_APP_SECRET)
    throw new Error('META_APP_ID / META_APP_SECRET not configured');

  const q = new URLSearchParams({
    grant_type:        'fb_exchange_token',
    client_id:         process.env.META_APP_ID,
    client_secret:     process.env.META_APP_SECRET,
    fb_exchange_token: shortToken,
  });
  const res = await fetch(`${GRAPH}/oauth/access_token?${q}`);
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(`Long-lived token: ${data.error?.message || res.status}`);
  // data.expires_in is in seconds
  return data; // { access_token, token_type, expires_in }
}

// Refresh a long-lived token (can be called when < 15 days remain)
async function refreshLongLivedToken(currentToken) {
  return getLongLivedToken(currentToken);
}

// ── Facebook Pages + Instagram accounts ──────────────────────────────────────

// Returns all Facebook Pages the user manages, with their IG Business account if any.
// Each item: { page_id, page_name, page_access_token, ig_account_id, ig_username }
async function getPagesWithIG(userToken) {
  const data = await graphGet('/me/accounts', userToken, {
    fields: 'id,name,access_token,instagram_business_account{id,username}',
  });

  return (data.data || []).map(page => ({
    page_id:           page.id,
    page_name:         page.name,
    page_access_token: page.access_token,
    ig_account_id:     page.instagram_business_account?.id   || null,
    ig_username:       page.instagram_business_account?.username || null,
  }));
}

// Get FB user info (id + name)
async function getFBUser(userToken) {
  return graphGet('/me', userToken, { fields: 'id,name' });
}

// ── Token freshness ───────────────────────────────────────────────────────────

// Call before every Graph API use. Refreshes if < 15 days remain.
// Returns { token, updated, newExpires? }
async function ensureFreshToken(conn) {
  if (!conn.token_expires_at) return { token: conn.long_lived_token, updated: false };

  const expiresMs  = new Date(conn.token_expires_at).getTime();
  const fifteenDays = 15 * 24 * 60 * 60 * 1000;

  if (Date.now() + fifteenDays < expiresMs) {
    return { token: conn.long_lived_token, updated: false };
  }

  const data      = await refreshLongLivedToken(conn.long_lived_token);
  const newExpires = new Date(Date.now() + (data.expires_in || 5184000) * 1000).toISOString();
  return {
    token:      data.access_token,
    newExpires,
    updated:    true,
  };
}

module.exports = {
  buildOAuthURL,
  exchangeCode,
  getLongLivedToken,
  refreshLongLivedToken,
  getPagesWithIG,
  getFBUser,
  ensureFreshToken,
  SCOPES,
};
