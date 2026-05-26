'use strict';
const { sendChannelMessage } = require('./discord.service');

// Default templates used when no custom template is configured in DB
const DEFAULT_TEMPLATES = {
  weekly_report:    '📋 **Reporte Semanal**\n\n¡Hola {{nombre}}!\nYa podés completar tu reporte semanal.\n\n👉 {{link}}\n\nPor favor completalo antes del domingo a las 23:59 hs.',
  welcome:          '👋 **Bienvenido/a, {{nombre}}!** Este es tu canal privado donde vamos a poder acompañarte en el programa.\nAcá vas a recibir recordatorios, novedades y feedback del equipo.\n📋 Tu link de reporte semanal: {{link}}',
  reconnect:        '🔄 **{{nombre}} reconectó su Discord.** ¡Bienvenido/a de nuevo!',
  report_submitted: '📋 **Reporte enviado ✓** | Hola {{nombre}}, tu reporte de esta semana quedó registrado. El equipo lo revisará pronto.\n{{link}}',
  edit_approved:    '✅ **Edición aprobada** | Tenés 2 horas para actualizar tu reporte semanal.\n→ {{link}}',
  edit_rejected:    '❌ **Edición rechazada** | Tu solicitud de edición no fue aprobada por el equipo.',
};

const EVENT_LABELS = {
  weekly_report:    'Recordatorio semanal',
  welcome:          'Bienvenida (canal nuevo)',
  reconnect:        'Reconexión Discord',
  report_submitted: 'Reporte enviado',
  edit_approved:    'Edición aprobada',
  edit_rejected:    'Edición rechazada',
};

// Resolve a template for a given client + event, falling back to default
async function resolveTemplate(supabase, clienteId, event) {
  try {
    const { data } = await supabase
      .from('discord_templates')
      .select('template, enabled')
      .eq('cliente_id', clienteId)
      .eq('event', event)
      .maybeSingle();
    if (data && data.enabled !== false && data.template?.trim()) return data.template;
  } catch {}
  return DEFAULT_TEMPLATES[event] || '';
}

// Replace {{nombre}} and {{link}} in a template string
function applyVars(template, vars = {}) {
  return template
    .replace(/\{\{nombre\}\}/g, vars.nombre || '')
    .replace(/\{\{link\}\}/g,   vars.link   || '');
}

// In-memory dedup — resets on redeploy (acceptable: Railway rarely restarts mid-day)
const _fired = new Set();

async function _tick(supabase, frontendUrl) {
  const now         = new Date();
  const currentDay  = now.getUTCDay();
  const currentHour = now.getUTCHours();
  const currentMin  = now.getUTCMinutes();

  // Load all enabled Discord configs with their schedules
  const { data: configs, error } = await supabase
    .from('discord_config')
    .select('cliente_id, schedule_days, schedule_utc_hour, schedule_utc_min')
    .eq('enabled', true);

  if (error) { console.error('[Discord scheduler] DB error loading configs:', error.message); return; }
  if (!configs?.length) return;

  for (const cfg of configs) {
    const days = (cfg.schedule_days || '1,5').split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n));
    const hour = cfg.schedule_utc_hour ?? 12;
    const min  = cfg.schedule_utc_min  ?? 0;

    if (!days.includes(currentDay) || currentHour !== hour || currentMin !== min) continue;

    const key = `${cfg.cliente_id}-${now.toISOString().slice(0, 10)}`;
    if (_fired.has(key)) continue;
    _fired.add(key);

    console.log(`🔔 Discord scheduler — cliente_id=${cfg.cliente_id} day=${currentDay} ${currentHour}:${String(currentMin).padStart(2,'0')} UTC`);
    await _sendToClient(supabase, frontendUrl, cfg.cliente_id);
  }
}

async function _sendToClient(supabase, frontendUrl, clienteId) {
  const { data: alumnos, error } = await supabase
    .from('alumnos')
    .select('id, nombre, apellido, cliente_id, discord_channel_id')
    .eq('cliente_id', clienteId)
    .not('discord_channel_id', 'is', null);

  if (error) { console.error(`[Discord scheduler] DB error for cliente_id=${clienteId}:`, error.message); return; }
  if (!alumnos?.length) { console.log(`[Discord scheduler] No alumnos with Discord for cliente_id=${clienteId}`); return; }

  const template = await resolveTemplate(supabase, clienteId, 'weekly_report');
  const base     = (frontendUrl || '').replace(/\/$/, '');

  let sent = 0, failed = 0;
  for (const a of alumnos) {
    try {
      const nombre = [a.nombre, a.apellido].filter(Boolean).join(' ') || 'Alumno';
      const link   = base ? `${base}/formulario_semanal.html?cliente_id=${a.cliente_id}&alumno_id=${a.id}` : '';
      await sendChannelMessage(a.discord_channel_id, applyVars(template, { nombre, link }));
      sent++;
      console.log(`  ✅ Enviado → ${nombre} (canal ${a.discord_channel_id})`);
    } catch (err) {
      failed++;
      console.error(`  ❌ Error → alumno ${a.id} (${a.nombre || '?'}): ${err.message}`);
    }
  }
  console.log(`[Discord scheduler] cliente_id=${clienteId} — ${sent} enviados, ${failed} errores`);
  return { sent, failed };
}

// Send weekly reports to all enabled clients (used by manual trigger)
async function sendWeeklyReports(supabase, frontendUrl) {
  const { data: configs } = await supabase.from('discord_config').select('cliente_id').eq('enabled', true);
  if (!configs?.length) return { sent: 0, failed: 0, errors: [] };

  let totalSent = 0, totalFailed = 0;
  for (const cfg of configs) {
    const r = await _sendToClient(supabase, frontendUrl, cfg.cliente_id);
    totalSent   += r?.sent   || 0;
    totalFailed += r?.failed || 0;
  }
  return { sent: totalSent, failed: totalFailed, errors: [] };
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
  console.log('🕐 Discord scheduler started — horario dinámico por cliente desde discord_config');
}

// Legacy alias
async function triggerReminder(supabase, frontendUrl) {
  return sendWeeklyReports(supabase, frontendUrl);
}

module.exports = { startScheduler, sendWeeklyReports, triggerReminder, resolveTemplate, applyVars, DEFAULT_TEMPLATES, EVENT_LABELS };
