'use strict';
const crypto = require('crypto');

// Verify Calendly HMAC-SHA256 webhook signature
// Header format: "t=<timestamp>,v1=<hex_signature>"
function verifySignature(rawBody, sigHeader, signingKey) {
  if (!signingKey) return true; // dev mode: skip verification
  if (!sigHeader)  return false;
  const parts    = Object.fromEntries(
    sigHeader.split(',').map(p => { const i = p.indexOf('='); return [p.slice(0, i), p.slice(i + 1)]; })
  );
  const t        = parts.t;
  const received = parts.v1;
  if (!t || !received) return false;
  const expected = crypto.createHmac('sha256', signingKey).update(`${t}.${rawBody}`).digest('hex');
  try {
    // timingSafeEqual requires same-length buffers
    const a = Buffer.from(expected,  'hex');
    const b = Buffer.from(received.padEnd(expected.length, '0'), 'hex').slice(0, a.length);
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch { return false; }
}

// Extract invitee data from Calendly v2 webhook payload
// payload = the top-level "payload" object in the webhook body
function extractInvitee(payload) {
  // v2: payload IS the invitee; v1 legacy: payload may have payload.invitee subobject
  const inv = (payload.invitee && typeof payload.invitee === 'object') ? payload.invitee : payload;

  const name      = inv.name  || [inv.first_name, inv.last_name].filter(Boolean).join(' ') || '';
  const email     = inv.email || '';
  const uri       = inv.uri   || '';

  // v2: scheduled_event is on the invitee object; v1: it's a sibling of invitee in payload
  const event    = inv.scheduled_event || payload.scheduled_event || {};
  const location = event.location || {};

  // event_type URI: v2 → string; v1 → object with uuid field
  let eventTypeUri = event.event_type || null;
  if (!eventTypeUri && payload.event_type?.uuid) {
    eventTypeUri = `https://api.calendly.com/event_types/${payload.event_type.uuid}`;
  }

  const startTime   = event.start_time || null;
  const meetingLink = location.join_url || (location.data && location.data.join_url) || null;

  // Old invitee URI (rescheduled events)
  const oldInviteeUri = typeof inv.old_invitee === 'string'
    ? inv.old_invitee
    : (inv.old_invitee?.uri || null);

  // Phone detection + form responses
  let telefono = '';
  const formResponses = {};
  const qas = inv.questions_and_answers || payload.questions_and_answers || [];
  for (const qa of qas) {
    const q = (qa.question || '').trim();
    const a = (qa.answer   || '').trim();
    if (!q) continue;
    formResponses[q] = a;
    const ql = q.toLowerCase();
    if (!telefono && (
      ql.includes('phone') || ql.includes('teléfono') || ql.includes('telefono') ||
      ql.includes('celular') || ql.includes('whatsapp') || ql.includes('movil') || ql.includes('móvil')
    )) telefono = a;
  }

  return { name, email, telefono, uri, eventTypeUri, startTime, meetingLink, formResponses, oldInviteeUri };
}

module.exports = { verifySignature, extractInvitee };
