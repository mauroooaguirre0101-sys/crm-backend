'use strict';
const { sendChannelMessage } = require('./discord.service');

// Monday 09:00 Uruguay (UTC-3) = Mon 12:00 UTC
// Friday  09:00 Uruguay (UTC-3) = Fri 12:00 UTC
const TRIGGERS = [
  { day: 1, utcHour: 12, utcMin: 0, type: 'monday' },
  { day: 5, utcHour: 12, utcMin: 0, type: 'friday' },
];

// In-memory dedup — resets on redeploy (acceptable: Railway rarely restarts mid-day)
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

    console.log(`🔔 Discord scheduler firing: ${t.type} (${now.toISOString()})`);
    await sendWeeklyReports(supabase, frontendUrl);
  }
}

// Core send function — reused by scheduler and manual endpoint
async function sendWeeklyReports(supabase, frontendUrl) {
  // Only alumnos with both fields set
  const { data: alumnos, error } = await supabase
    .from('alumnos')
    .select('id, nombre, apellido, cliente_id, discord_user_id, discord_channel_id')
    .not('discord_user_id',   'is', null)
    .not('discord_channel_id', 'is', null);

  if (error) {
    console.error('❌ Discord scheduler — DB error:', error.message);
    return { sent: 0, failed: 0, errors: [] };
  }

  if (!alumnos?.length) {
    console.log('ℹ️  Discord scheduler — no alumnos with Discord connected');
    return { sent: 0, failed: 0, errors: [] };
  }

  const base = (frontendUrl || '').replace(/\/$/, '');

  let sent   = 0;
  let failed = 0;
  const errors = [];

  console.log(`📤 Discord scheduler — sending to ${alumnos.length} alumnos…`);

  for (const a of alumnos) {
    try {
      const nombre = [a.nombre, a.apellido].filter(Boolean).join(' ') || 'Alumno';
      const link   = base
        ? `${base}/formulario_semanal.html?cliente_id=${a.cliente_id}&alumno_id=${a.id}`
        : '';

      const msg =
        `📋 **Reporte Semanal**\n\n` +
        `¡Hola ${nombre}!\n` +
        `Ya podés completar tu reporte semanal.\n\n` +
        (link ? `👉 ${link}\n\n` : '') +
        `Por favor completalo antes del domingo a las 23:59 hs.`;

      await sendChannelMessage(a.discord_channel_id, msg);
      sent++;
      console.log(`  ✅ Enviado → ${nombre} (canal ${a.discord_channel_id})`);
    } catch (err) {
      failed++;
      const isInvalidChannel = err.message.includes('10003') || err.message.includes('Unknown Channel');
      const reason = isInvalidChannel ? 'canal inválido o eliminado' : err.message;
      errors.push({ alumno_id: a.id, nombre: a.nombre, reason });
      console.error(`  ❌ Error → alumno ${a.id} (${a.nombre || '?'}): ${reason}`);
    }
  }

  console.log(`📊 Discord scheduler — resultado: ${sent} enviados, ${failed} errores`);
  return { sent, failed, errors };
}

function startScheduler(supabase, frontendUrl) {
  if (!process.env.DISCORD_BOT_TOKEN) {
    console.log('⚠️  Discord scheduler: DISCORD_BOT_TOKEN missing, skipping');
    return;
  }
  setInterval(
    () => _tick(supabase, frontendUrl).catch(err => console.error('Scheduler tick error:', err.message)),
    60_000
  );
  console.log('🕐 Discord scheduler started — Mon 12:00 UTC / Fri 12:00 UTC (09:00 Uruguay)');
}

// Legacy alias kept for backward compatibility
async function triggerReminder(supabase, frontendUrl) {
  return sendWeeklyReports(supabase, frontendUrl);
}

module.exports = { startScheduler, sendWeeklyReports, triggerReminder };
