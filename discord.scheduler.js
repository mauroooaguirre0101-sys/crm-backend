'use strict';
const { sendChannelMessage } = require('./discord.service');

// Monday 09:00 ART (UTC-3) = Mon 12:00 UTC
// Friday  18:00 ART (UTC-3) = Fri 21:00 UTC
const TRIGGERS = [
  { day: 1, utcHour: 12, utcMin: 0, type: 'monday' },
  { day: 5, utcHour: 21, utcMin: 0, type: 'friday' },
];

// Track which triggers already fired today (in-memory, resets on redeploy — acceptable)
const _fired = new Set();

async function _tick(supabase, frontendUrl) {
  const now = new Date();
  for (const t of TRIGGERS) {
    if (
      now.getUTCDay()     !== t.day     ||
      now.getUTCHours()   !== t.utcHour ||
      now.getUTCMinutes() !== t.utcMin
    ) continue;

    const key = `${t.type}-${now.toISOString().slice(0, 10)}`;
    if (_fired.has(key)) continue;
    _fired.add(key);

    console.log(`🔔 Discord scheduler firing: ${t.type}`);
    await _sendReminders(supabase, frontendUrl, t.type);
  }
}

async function _sendReminders(supabase, frontendUrl, type) {
  const { data: alumnos, error } = await supabase
    .from('alumnos')
    .select('id, nombre, apellido, cliente_id, discord_channel_id')
    .not('discord_channel_id', 'is', null);

  if (error || !alumnos?.length) {
    if (error) console.error('Scheduler DB error:', error.message);
    return;
  }

  const base = (frontendUrl || '').replace(/\/$/, '');

  for (const a of alumnos) {
    try {
      const nombre = [a.nombre, a.apellido].filter(Boolean).join(' ') || 'Hola';
      const link   = base
        ? `${base}/formulario_semanal.html?cliente_id=${a.cliente_id}&alumno_id=${a.id}`
        : '';

      const msg = type === 'monday'
        ? `📋 **Recordatorio semanal** | Hola ${nombre}! Arrancó la semana — completá tu reporte cuando puedas.\n${link}`
        : `⏰ **Último aviso** | ${nombre}, hoy es viernes. No olvides entregar tu reporte antes de que cierre la semana.\n${link}`;

      await sendChannelMessage(a.discord_channel_id, msg);
    } catch (err) {
      console.error(`Scheduler: alumno ${a.id}:`, err.message);
    }
  }
}

// Manual trigger — useful for testing via an admin endpoint
async function triggerReminder(supabase, frontendUrl, type) {
  await _sendReminders(supabase, frontendUrl, type);
}

function startScheduler(supabase, frontendUrl) {
  if (!process.env.DISCORD_BOT_TOKEN) {
    console.log('⚠️  Discord scheduler: DISCORD_BOT_TOKEN missing, skipping');
    return;
  }
  // Check every minute for triggers
  setInterval(
    () => _tick(supabase, frontendUrl).catch(err => console.error('Scheduler error:', err.message)),
    60_000
  );
  console.log('🕐 Discord scheduler started (Mon 12:00 UTC / Fri 21:00 UTC)');
}

module.exports = { startScheduler, triggerReminder };
