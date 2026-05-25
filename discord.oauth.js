'use strict';
const DISCORD_API = 'https://discord.com/api/v10';
const SCOPES      = 'identify guilds.join';

// Encode alumno context + return destination in the OAuth state (base64url JSON)
function buildState(alumno_id, cliente_id, return_to = 'formulario') {
  return Buffer.from(JSON.stringify({ alumno_id, cliente_id, return_to })).toString('base64url');
}

function parseState(state) {
  try {
    return JSON.parse(Buffer.from(state, 'base64url').toString());
  } catch { return null; }
}

// Redirect URL to send the user to Discord's consent screen
function getOAuthURL(alumno_id, cliente_id, return_to = 'formulario') {
  const params = new URLSearchParams({
    client_id:     process.env.DISCORD_CLIENT_ID,
    redirect_uri:  process.env.DISCORD_REDIRECT_URI,
    response_type: 'code',
    scope:         SCOPES,
    state:         buildState(alumno_id, cliente_id, return_to),
    prompt:        'consent',
  });
  return `https://discord.com/api/oauth2/authorize?${params}`;
}

// Exchange authorization code for access token
async function exchangeCode(code) {
  const res = await fetch(`${DISCORD_API}/oauth2/token`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    new URLSearchParams({
      client_id:     process.env.DISCORD_CLIENT_ID,
      client_secret: process.env.DISCORD_CLIENT_SECRET,
      grant_type:    'authorization_code',
      code,
      redirect_uri:  process.env.DISCORD_REDIRECT_URI,
    }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token exchange [${res.status}]: ${text}`);
  }
  return res.json();
}

// Get the authenticated Discord user's profile
async function getDiscordUser(accessToken) {
  const res = await fetch(`${DISCORD_API}/users/@me`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) throw new Error(`getDiscordUser [${res.status}]`);
  return res.json();
}

module.exports = { getOAuthURL, exchangeCode, getDiscordUser, parseState };
