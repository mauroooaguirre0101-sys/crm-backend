const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();

// ✅ Middlewares
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '25mb', verify: (req, _res, buf) => { req.rawBody = buf; } }));

// 🔑 Supabase (ws transport required for realtime on Node < 22)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY,
  {
    realtime: {
      transport: require('ws'),
    },
  }
);

// 📧 Gmail + Nodemailer
async function sendSessionEmail(alumno, sesion, clienteId) {
  console.log('📧 sendSessionEmail → alumno:', alumno?.email, '| gmail:', process.env.GMAIL_USER ? 'OK' : 'MISSING');
  if (!process.env.GMAIL_USER || !process.env.GMAIL_PASS || !alumno?.email) {
    console.log('📧 Skipping: missing config or email');
    return;
  }

  const nombre = [alumno.nombre, alumno.apellido].filter(Boolean).join(' ') || 'Alumno';
  const [y, m, d] = sesion.fecha.split('-');
  const fechaObj = new Date(Number(y), Number(m) - 1, Number(d));
  const fechaLegible = fechaObj.toLocaleDateString('es-AR', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });
  const hora = sesion.hora ? sesion.hora.slice(0, 5) : null;

  const frontendUrl = (process.env.FRONTEND_URL || '').replace(/\/$/, '');
  const formLink = frontendUrl
    ? `${frontendUrl}/formulario_semanal.html?cliente_id=${encodeURIComponent(clienteId)}&alumno_id=${encodeURIComponent(alumno.id)}`
    : null;

  const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: { user: process.env.GMAIL_USER, pass: process.env.GMAIL_PASS },
  });

  const fromAddress = `CRM Sesiones <${process.env.GMAIL_USER}>`;

  const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f8;font-family:Inter,Arial,sans-serif">
  <div style="max-width:520px;margin:40px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.08)">
    <div style="background:#0a0b0f;padding:22px 32px">
      <div style="font-size:17px;font-weight:800;color:#e0b54a;letter-spacing:-.3px">📅 Sesión Programada</div>
    </div>
    <div style="padding:28px 32px">
      <p style="font-size:16px;font-weight:700;margin:0 0 10px;color:#111">Hola ${nombre}!</p>
      <p style="font-size:14px;color:#555;margin:0 0 22px;line-height:1.6">Tu consultor programó una sesión para vos:</p>
      <div style="background:#f8f8fb;border:1px solid #e8e8f0;border-left:4px solid #e0b54a;border-radius:10px;padding:16px 20px;margin-bottom:24px">
        <div style="font-size:11px;color:#999;font-weight:700;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px">Fecha y hora</div>
        <div style="font-size:19px;font-weight:700;color:#111;text-transform:capitalize">${fechaLegible}</div>
        ${hora ? `<div style="font-size:14px;color:#666;margin-top:4px;font-weight:600">${hora} hs</div>` : ''}
      </div>
      ${formLink ? `
      <p style="font-size:13px;color:#555;margin:0 0 16px;line-height:1.6">Para aprovechar mejor el tiempo, completá tu reporte semanal antes de que empecemos:</p>
      <a href="${formLink}" style="display:inline-block;background:#e0b54a;color:#000;font-weight:700;font-size:14px;padding:12px 26px;border-radius:8px;text-decoration:none">Completar reporte semanal →</a>
      ` : ''}
    </div>
    <div style="padding:14px 32px 20px;border-top:1px solid #f0f0f0">
      <p style="font-size:11px;color:#bbb;margin:0">Este mensaje fue enviado automáticamente por tu consultor.</p>
    </div>
  </div>
</body></html>`;

  try {
    const result = await transporter.sendMail({
      from: fromAddress,
      to: alumno.email,
      subject: `📅 Sesión programada — ${fechaLegible.replace(/^\w/, c => c.toUpperCase())}`,
      html,
    });
    console.log('📧 Email enviado:', result.messageId);
  } catch (err) {
    console.error('📧 Error al enviar email:', err.message);
  }
}

// ===============================
// 🔐 LOGIN MULTI-CLIENTE
// ===============================
app.post('/login', async (req, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Falta email' });
    }

    const { data: clientes, error } = await supabase
      .from('user_clientes')
      .select('*')
      .eq('user_email', email);

    if (error || !clientes || clientes.length === 0) {
      return res.status(403).json({ error: 'Usuario sin clientes asignados' });
    }

    res.json({
      email,
      clientes
    });

  } catch (err) {
    console.error('❌ LOGIN ERROR:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});

// ===============================
// 🔐 MIDDLEWARE MULTI-CLIENTE REAL
// ===============================
async function validateAccess(req, res, next) {
  try {
    const cliente_id = req.headers['x-cliente-id'];
    const email = req.headers['x-user-email'];

    if (!cliente_id || !email) {
      return res.status(400).json({ error: 'Faltan headers' });
    }

    const { data, error } = await supabase
      .from('user_clientes')
      .select('*')
      .eq('user_email', email)
      .eq('cliente_id', cliente_id)
      .single();

    if (error || !data) {
      return res.status(403).json({ error: 'Sin acceso a este cliente' });
    }

    req.cliente_id = cliente_id;
    req.user = data;

    next();

  } catch (err) {
    console.error('❌ Error auth:', err);
    res.status(500).json({ error: 'Error de autenticación' });
  }
}

// ===============================
// 🔐 HELPER: checkAccess (función pura para rutas que no usan middleware)
// ===============================
async function checkAccess(req) {
  const cliente_id = req.headers['x-cliente-id'];
  const email      = req.headers['x-user-email'];
  if (!cliente_id || !email) {
    return { ok: false, status: 400, error: 'Faltan headers x-cliente-id / x-user-email' };
  }
  const { data, error } = await supabase
    .from('user_clientes')
    .select('*')
    .eq('user_email', email)
    .eq('cliente_id', cliente_id)
    .single();
  if (error || !data) {
    return { ok: false, status: 403, error: 'Sin acceso a este cliente' };
  }
  return { ok: true, cliente_id, user: data };
}

// 🤖 Anthropic client (initialized lazily from env var)
const _anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

// 🎮 Discord modules
const _discord          = require('./discord.service');
const _discordOAuth     = require('./discord.oauth');
const { startScheduler: _startDiscordScheduler, triggerReminder: _triggerDiscordReminder, sendWeeklyReports: _sendWeeklyReports } = require('./discord.scheduler');
const { startGateway: _startDiscordGateway } = require('./discord.gateway');
const { verifySignature: _calendlyVerify, extractInvitee: _calendlyExtract } = require('./calendly.service');
const _calendlyOAuth = require('./calendly.oauth');

const AI_BASE_SYSTEM = `Sos el coach de ventas más exigente y preciso del mercado hispanohablante. Analizás llamadas de alto ticket con estándares quirúrgicos. Tu trabajo no es motivar — es diagnosticar con precisión clínica, señalar exactamente qué falló y dar alternativas concretas de ejecución.

PRINCIPIOS DE EVALUACIÓN:
- Sé brutalmente honesto. Un score de 7 significa "sólido". Un 5 significa "aceptable pero con problemas serios". Un 3 significa "falló esta fase". No infles scores para no herir sentimientos.
- Cada crítica debe incluir: qué exactamente falló + la alternativa concreta (entre comillas) de lo que DEBERÍA haberse dicho o hecho.
- Cada fortaleza debe ser genuina y específica, no genérica ("mantuvo tono amigable" no vale — explicá por qué importó en esa llamada específica).
- Si el vendedor habló más de 5 minutos seguidos sin hacer preguntas, marcalo como error crítico.
- Si no hubo próximo paso concreto al final, score de Cierre máximo 3.
- Si no hubo descubrimiento real antes del pitch, score de Descubrimiento máximo 4.

---

PARTE 1 — SCORECARD ESTRUCTURADO (siempre primero, antes del análisis narrativo):

Comenzá tu respuesta con exactamente este bloque (el JSON debe ser válido, sin texto antes):

__SCORECARD__
{
  "score_global": 0.0,
  "summary": "2-3 oraciones de resumen ejecutivo para el scorecard",
  "phases": [
    {"id":"hits","score":0,"good":["fortaleza específica con ejemplo de la llamada"],"improve":["qué exactamente falló + entre comillas la alternativa concreta que debería haber usado"]},
    {"id":"rapport","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"desarrollo","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"descubrimiento","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"prepitch","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"pitch","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"solucion","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"presentacion","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"cierre","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]},
    {"id":"objeciones","score":0,"good":["fortaleza específica"],"improve":["alternativa concreta entre comillas"]}
  ],
  "actions": [
    {"title":"Título de la acción", "desc":"Instrucción exacta de cómo ejecutarla en la próxima llamada, con ejemplos de frases o preguntas específicas"},
    {"title":"Título de la acción", "desc":"Instrucción exacta"},
    {"title":"Título de la acción", "desc":"Instrucción exacta"},
    {"title":"Título de la acción", "desc":"Instrucción exacta"},
    {"title":"Título de la acción", "desc":"Instrucción exacta"}
  ],
  "impactTitle": "La fase o error con mayor potencial de mejora inmediata",
  "impactDesc": "Explicación de por qué esta fase fue la más costosa y qué resultado concreto y medible se espera al mejorarla en la próxima llamada"
}
__/SCORECARD__

---

PARTE 2 — ANÁLISIS NARRATIVO (markdown, después del scorecard):

## Resumen ejecutivo
(2-3 oraciones directas: qué pasó, cuál fue el resultado real, cuál fue el error más costoso)

## Dolores detectados
(bullets con los problemas que mencionó el prospecto, con citas textuales entre comillas cuando sea posible)

## Objeciones identificadas
(para cada objeción: qué dijo el prospecto — cómo lo manejó el vendedor — qué debería haber dicho/hecho)

## Señales de compra desaprovechadas
(momentos donde el prospecto mostró interés y el vendedor no lo capitalizó)

## Señales de alarma
(señales de riesgo, frialdad, comparación con competidores, falta de urgencia)

## Errores críticos de la llamada
(los 2-3 errores más costosos, con explicación de por qué afectaron el resultado)

## Próximos pasos
(acciones concretas ordenadas por impacto)

---

REGLAS ESTRICTAS DEL SCORECARD:
- score_global: promedio real de los 10 scores con un decimal. No redondees hacia arriba.
- Los scores van del 1 al 10 (enteros). Calibración: 8-10 = excelente ejecución, 6-7 = correcto con margen de mejora, 4-5 = problemas claros que costaron el cierre, 1-3 = fase fallida o ausente.
- Si una fase no ocurrió en absoluto: score 1, explicalo en "improve".
- "good": mínimo 1, máximo 2 ítems. Tienen que ser observaciones reales de esa llamada, no genéricas.
- "improve": mínimo 1, máximo 3 ítems. Cada uno DEBE incluir la alternativa concreta entre comillas.
- "actions": exactamente 5 acciones, ordenadas de mayor a menor impacto en el resultado.
- El JSON debe ser válido. Sin comentarios dentro del JSON.

Para preguntas de seguimiento respondés de forma conversacional y directa, sin repetir la estructura completa. Siempre respondés en español.`;

// Fases del scorecard para el frontend (alineadas con PDF analyzer)
const CALL_PHASE_DEFS = [
  { id: 'hits',          name: 'Hits',          icon: '🎯' },
  { id: 'rapport',       name: 'Rapport',       icon: '🤝' },
  { id: 'desarrollo',    name: 'Desarrollo',    icon: '🔍' },
  { id: 'descubrimiento',name: 'Descubrimiento',icon: '💡' },
  { id: 'prepitch',      name: 'Pre pitch',     icon: '📍' },
  { id: 'pitch',         name: 'Pitch',         icon: '🎤' },
  { id: 'solucion',      name: 'Solución',      icon: '🔧' },
  { id: 'presentacion',  name: 'Presentación',  icon: '📊' },
  { id: 'cierre',        name: 'Cierre',        icon: '✅' },
  { id: 'objeciones',    name: 'Objeciones',    icon: '🛡' },
];

// Fases del scorecard para el frontend
const GHL_CALL_PHASES = [
  { id: 'apertura',     name: 'Apertura',      icon: '🎯' },
  { id: 'rapport',      name: 'Rapport',        icon: '🤝' },
  { id: 'diagnostico',  name: 'Diagnóstico',    icon: '🔍' },
  { id: 'agitacion',    name: 'Agitación',      icon: '💥' },
  { id: 'vision',       name: 'Visión',         icon: '✨' },
  { id: 'calificacion', name: 'Calificación',   icon: '💎' },
  { id: 'pitch',        name: 'Pitch',          icon: '🎤' },
  { id: 'objeciones',   name: 'Objeciones',     icon: '🛡' },
  { id: 'cierre',       name: 'Cierre',         icon: '✅' },
  { id: 'compromiso',   name: 'Compromiso',     icon: '📌' },
];

function _parseCallScorecard(text) {
  try {
    const match = text.match(/__SCORECARD__\s*([\s\S]*?)\s*__\/SCORECARD__/);
    if (!match) return null;
    // Strip markdown code block wrapping if the AI added ```json ... ```
    const raw = match[1].replace(/^```(?:json)?\s*/,'').replace(/\s*```\s*$/,'').trim();
    return JSON.parse(raw);
  } catch { return null; }
}

function _stripScorecardBlock(text) {
  return text.replace(/__SCORECARD__[\s\S]*?__\/SCORECARD__/g, '').trim();
}

const CHAT_BASE_SYSTEM = `Sos un analista estratégico de ventas especializado en conversaciones de prospección por chat (Instagram DM, WhatsApp, etc.) para negocios de alto ticket en el mercado hispanohablante. Tu trabajo es analizar conversaciones entre setters/vendedores y leads para identificar oportunidades, errores y estrategias de mejora.

REGLA FUNDAMENTAL: Siempre respondés con valor concreto, sin importar cuánta información tengas. Si tenés el chat completo, analizás en detalle. Si tenés solo contexto, descripción o una situación parcial, igualmente dás insights accionables, estrategia y recomendaciones basadas en lo que se compartió. Nunca pedís más información antes de responder — primero analizás con lo que hay, y al final podés sugerir qué más ayudaría.

Cuando analizás una conversación por primera vez, usás las secciones que apliquen según la información disponible. Si hay poco contexto, priorizás las secciones más útiles y las otras las omitís o las respondés con lo que se puede inferir:

## Resumen ejecutivo
(contexto del chat, etapa del embudo, estado actual del lead — con lo que haya)

## Objeciones detectadas
(si hay mensajes: bullets con cita + tipo + cómo se manejó. Si es solo contexto: objeciones probables para esta situación)

## Errores del setter/vendedor
(mensajes que generaron fricción, oportunidades perdidas, presión mal timed — o errores probables si no hay chat literal)

## Análisis emocional del lead
(tono dominante y señales de interés según lo compartido)

## Nivel de interés y calificación
(puntuación 1-10 con justificación, si vale continuar)

## Señales de compra detectadas
(señales positivas concretas del chat o del contexto dado)

## Oportunidades perdidas
(momentos donde se debería haber actuado diferente — o qué no hacer en esta situación)

## Mejores respuestas recomendadas
(versiones mejoradas de mensajes fallidos, o respuestas ideales para la situación descrita)

## Estrategia de seguimiento
(mensaje concreto recomendado para el próximo contacto, cuándo enviarlo y por qué)

Para preguntas de seguimiento respondés de forma conversacional y directa, sin repetir la estructura completa. Si hay imágenes, las analizás también. Siempre respondés en español.`;

// 🟢 Test
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});


// ===============================
// 🔥 GET LEADS
// ===============================
// Fields returned for the stats cache (leadsCache) — excludes large text blobs
const LEADS_LITE_FIELDS = [
  'id','estado','estado_anterior','calificado','descalificado','tipo','origen',
  'created_at','updated_at','estado_updated_at',
  'etiqueta','etiquetas','nombre','instagram',
  'source','seguimientos','show','respondio_seguimiento_4',
].join(',');

app.get('/leads', validateAccess, async (req, res) => {
  try {
    const { after, lite, page, per_page, estado, search, period, mes, vista, sort_by, sort_dir, etiqueta_filter, date_from, date_to } = req.query;

    // ── Incremental mode: returns only leads changed since `after` ──
    if (after) {
      const fields = lite === '1' ? LEADS_LITE_FIELDS : '*';
      let q = supabase.from('leads').select(fields)
        .eq('cliente_id', req.cliente_id)
        .gt('updated_at', after)
        .order('updated_at', { ascending: false })
        .limit(500);
      const { data, error } = await q;
      if (error) { console.error('❌ GET LEADS incremental:', error); return res.status(500).json({ error: error.message }); }
      return res.json(data);
    }

    // ── Lite mode (no page param): returns ALL leads with minimal fields for stats ──
    // Uses range-based batching to overcome Supabase's default 1000-row max-rows limit
    if (lite === '1' && !page) {
      const BATCH = 1000;
      const MAX   = 20000;
      let allData = [];
      let from    = 0;
      while (allData.length < MAX) {
        const { data, error } = await supabase.from('leads')
          .select(LEADS_LITE_FIELDS)
          .eq('cliente_id', req.cliente_id)
          .order('created_at', { ascending: false })
          .range(from, from + BATCH - 1);
        if (error) { console.error('❌ GET LEADS lite:', error); return res.status(500).json({ error: error.message }); }
        if (!data || data.length === 0) break;
        allData = allData.concat(data);
        if (data.length < BATCH) break;
        from += BATCH;
      }
      return res.json(allData);
    }

    // ── Paginated full mode: returns one page of full leads ──
    const pageNum  = Math.max(1, parseInt(page)     || 1);
    const perPage  = Math.min(parseInt(per_page)    || 100, 200);
    const offset   = (pageNum - 1) * perPage;

    const orderField = sort_by === 'updated_at' ? 'updated_at' : 'created_at';
    const ascending  = sort_dir === 'asc';
    let q = supabase.from('leads')
      .select('*', { count: 'exact' })
      .eq('cliente_id', req.cliente_id)
      .order(orderField, { ascending })
      .range(offset, offset + perPage - 1);

    if (estado)               q = q.eq('estado', estado);
    if (search && search.trim()) q = q.or(`nombre.ilike.%${search.trim()}%,instagram.ilike.%${search.trim()}%`);
    if (etiqueta_filter && etiqueta_filter.trim()) {
      const ef = etiqueta_filter.trim();
      q = q.or(`etiqueta.eq.${ef},etiquetas.cs.["${ef}"]`);
    }
    if (vista === 'perdidos')  q = q.or('estado.eq.Perdido,and(seguimientos.gte.4,respondio_seguimiento_4.eq.NO)');
    if (vista === 'activos')   q = q.neq('estado', 'Perdido');

    const now = new Date();
    if (date_from || date_to) {
      if (date_from) q = q.gte('created_at', new Date(date_from + 'T00:00:00').toISOString());
      if (date_to)   q = q.lte('created_at', new Date(date_to   + 'T23:59:59').toISOString());
    } else if (mes !== undefined && mes !== '') {
      const m  = parseInt(mes, 10);
      const yr = now.getFullYear();
      q = q.gte('created_at', new Date(yr, m, 1).toISOString())
           .lte('created_at', new Date(yr, m + 1, 0, 23, 59, 59).toISOString());
    } else if (period) {
      if (period === 'dia') {
        const from = new Date(now); from.setHours(0, 0, 0, 0);
        q = q.gte('created_at', from.toISOString());
      } else if (period === 'semana') {
        const dow = now.getDay();
        const from = new Date(now); from.setDate(now.getDate() - (dow === 0 ? 6 : dow - 1)); from.setHours(0, 0, 0, 0);
        q = q.gte('created_at', from.toISOString());
      } else if (period === 'mes') {
        const from = new Date(now.getFullYear(), now.getMonth(), 1);
        const to   = new Date(now.getFullYear(), now.getMonth() + 1, 1);
        q = q.gte('created_at', from.toISOString()).lt('created_at', to.toISOString());
      } else if (period === 'año') {
        const from = new Date(now); from.setFullYear(now.getFullYear() - 1); from.setHours(0, 0, 0, 0);
        q = q.gte('created_at', from.toISOString());
      }
    }

    const { data, error, count } = await q;
    if (error) { console.error('❌ GET LEADS paged:', error); return res.status(500).json({ error: error.message }); }

    res.json({ leads: data, total: count, page: pageNum, per_page: perPage });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 CREATE LEAD (manual)
// ===============================
app.post('/leads', validateAccess, async (req, res) => {
  try {
    const {
      nombre, instagram, origen, tipo, etiqueta, etiquetas,
      estado, ultima_accion, notas, seguimientos,
      source, updated_at, estado_updated_at, created_at,
    } = req.body;

    if (!nombre || !nombre.trim()) {
      return res.status(400).json({ error: 'Falta nombre' });
    }

    const now = new Date().toISOString();
    const etiquetasArr = Array.isArray(etiquetas) ? etiquetas
      : (etiqueta ? [etiqueta] : []);

    // Core fields — definitely exist in the schema
    const coreRow = {
      nombre:        nombre.trim(),
      instagram:     instagram ? instagram.trim().replace(/^@/, '').toLowerCase() : '',
      origen:        ['Inbound','Outbound'].includes(origen) ? origen : 'Inbound',
      tipo:          ['Ads','Organico','Outbound'].includes(tipo) ? tipo : 'Organico',
      etiqueta:      etiqueta || '',
      estado:        estado || 'Primer contacto',
      ultima_accion: ultima_accion || '',
      notas:         notas || '',
      source:        source || 'manual',
      created_at:    created_at || now,
      updated_at:    updated_at || now,
      cliente_id:    req.cliente_id,
    };

    // Try full insert including newer columns first
    const fullRow = {
      ...coreRow,
      seguimientos:      parseInt(seguimientos) || 0,
      estado_updated_at: estado_updated_at || now,
      etiquetas:         etiquetasArr,
    };

    let { error } = await supabase.from('leads').insert(fullRow);

    // If newer columns don't exist yet, fall back to core fields only
    if (error && (
      error.message.includes('seguimientos') ||
      error.message.includes('estado_updated_at') ||
      error.message.includes('etiquetas')
    )) {
      console.warn('⚠️ Columnas nuevas no encontradas, insertando sin ellas:', error.message);
      const result2 = await supabase.from('leads').insert(coreRow);
      error = result2.error;
    }

    if (error) {
      console.error('❌ INSERT LEAD:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 UPDATE LEAD
// ===============================
app.patch('/leads/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    const updates = { ...req.body };

    delete updates.id;
    delete updates.cliente_id;
    delete updates.created_at;

    // Non-admins can only set agendado_por if it's not already set
    if (updates.agendado_por !== undefined && req.user.role !== 'admin') {
      try {
        const { data: current } = await supabase.from('leads').select('agendado_por').eq('id', id).eq('cliente_id', req.cliente_id).single();
        if (current?.agendado_por) delete updates.agendado_por;
      } catch (_) { delete updates.agendado_por; }
    }

    const { error } = await supabase
      .from('leads')
      .update(updates)
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ UPDATE LEAD:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 DELETE LEAD
// ===============================
app.delete('/leads/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    console.log('[DELETE LEAD] id:', id, 'cliente_id:', req.cliente_id);

    const { error } = await supabase
      .from('leads')
      .delete()
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ DELETE LEAD:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER DELETE LEAD:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 GET CALLS
// ===============================
app.get('/calls', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('calls')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('❌ GET CALLS:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 PRE-CALL
// ===============================
app.post('/call/precall', validateAccess, async (req, res) => {
  try {
    const { nombre, instagram, whatsapp, info_previa, origen, fecha_llamada,
            closer, calendar_name, email, agendado_por } = req.body;

    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    const { data: existingCalls, error: fetchError } = await supabase
      .from('calls')
      .select('id')
      .eq('instagram', instagram)
      .eq('cliente_id', req.cliente_id);

    if (fetchError) {
      return res.status(500).json({ error: fetchError.message });
    }

    const numero_llamada = (existingCalls?.length || 0) + 1;

    const { error } = await supabase
      .from('calls')
      .insert({
        nombre: nombre || 'Sin nombre',
        instagram,
        whatsapp: whatsapp || '',
        info_previa: info_previa || '',
        origen: origen || '',
        estado: 'Pendiente',
        numero_llamada,
        seguimientos: 0,
        responde: false,
        cliente_id: req.cliente_id,
        ...(fecha_llamada   ? { fecha_llamada }   : {}),
        ...(closer          ? { closer }           : {}),
        ...(calendar_name   ? { calendar_name }    : {}),
        ...(email           ? { email }            : {})
      });

    if (error) {
      console.error('❌ PRECALL:', error);
      return res.status(500).json({ error: error.message });
    }

    // Propagate agendado_por to the matching lead if present (requires migration)
    if (agendado_por) {
      try {
        const ig = instagram.toLowerCase().replace(/^@/, '');
        const { data: matchLead } = await supabase
          .from('leads').select('id, agendado_por').eq('cliente_id', req.cliente_id)
          .ilike('instagram', ig).maybeSingle();
        if (matchLead && !matchLead.agendado_por) {
          await supabase.from('leads').update({ agendado_por }).eq('id', matchLead.id);
        }
      } catch (_) {}
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 UPDATE CALL
// ===============================
app.patch('/call/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    let {
      estado,
      motivo_no_cierre,
      seguimientos,
      responde,
      link_llamada,
      link_grabacion,
      reporte,
      info_previa,
      preguntas_calificacion,
      fecha_llamada,
      monto_sena,
      closer,
      notas_spc,
    } = req.body;

    motivo_no_cierre = motivo_no_cierre || '';
    link_llamada = link_llamada || '';
    link_grabacion = link_grabacion || '';
    reporte = reporte || '';
    seguimientos = parseInt(seguimientos) || 0;

    if (typeof responde === 'string') {
      responde = responde.toLowerCase() === 'si';
    } else {
      responde = Boolean(responde);
    }

    const { data: callData } = await supabase
      .from('calls')
      .select('*')
      .eq('id', id)
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();

    if (!callData) {
      return res.status(404).json({ error: 'Call no encontrada' });
    }

    const patch = {};
    if (estado           !== undefined) patch.estado            = estado;
    if (motivo_no_cierre !== undefined) patch.motivo_no_cierre  = motivo_no_cierre;
    if (seguimientos     !== undefined) patch.seguimientos       = seguimientos;
    if (responde         !== undefined) patch.responde           = responde;
    if (link_llamada     !== undefined) patch.link_llamada       = link_llamada;
    if (link_grabacion   !== undefined) patch.link_grabacion     = link_grabacion;
    if (reporte          !== undefined) patch.reporte            = reporte;
    if (info_previa      !== undefined) patch.info_previa        = info_previa;
    if (preguntas_calificacion !== undefined) patch.preguntas_calificacion = preguntas_calificacion;
    if ('fecha_llamada' in req.body)    patch.fecha_llamada      = fecha_llamada || null;
    if (monto_sena             !== undefined) patch.monto_sena           = monto_sena !== null ? parseFloat(monto_sena) : null;
    if (closer                 !== undefined) patch.closer               = closer || null;
    if (notas_spc              !== undefined) patch.notas_spc            = notas_spc || null;

    // Auto-registrar fecha de inicio de SPC cuando el estado cambia por primera vez
    if (estado === 'Seguimiento Post Call' && callData.estado !== 'Seguimiento Post Call') {
      patch.spc_date = new Date().toISOString();
    }

    // Auto-registrar cuándo se realizó la llamada (primera vez que sale de Pendiente/Re agenda)
    const ESTADOS_COMPLETADOS = ['Cierre','Cierre Cuotas','No Cierre','No asistió','Cancelada','Seña','Seguimiento Post Call'];
    if (estado && ESTADOS_COMPLETADOS.includes(estado) && !callData.fecha_realizada) {
      patch.fecha_realizada = new Date().toISOString();
    }

    const { error } = await supabase
      .from('calls')
      .update(patch)
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ UPDATE CALL:', error);
      return res.status(500).json({ error: error.message });
    }

    let nuevoEstadoLead = null;

    switch (estado) {
      case 'Cierre':
        nuevoEstadoLead = 'Cerrado';
        break;
      case 'Cierre PIF':
        nuevoEstadoLead = 'Cerrado';
        break;
      case 'Seña':
        nuevoEstadoLead = 'Seña';
        break;
      case 'Seguimiento Post Call':
        nuevoEstadoLead = 'Seguimiento Post Call';
        break;
      case 'Re agenda':
        nuevoEstadoLead = 'Re agendado';
        break;
      case 'No Cierre':
        nuevoEstadoLead = 'Perdido Post Call';
        break;
      case 'No asistió':
        nuevoEstadoLead = 'No Show';
        break;
      case 'Cancelada':
        nuevoEstadoLead = 'No Show';
        break;
    }

    if (nuevoEstadoLead) {
      let updated = false;
      if (callData.instagram) {
        const { data: matched } = await supabase
          .from('leads').select('id')
          .ilike('instagram', callData.instagram)
          .eq('cliente_id', req.cliente_id)
          .limit(1).maybeSingle();
        if (matched) {
          await supabase.from('leads').update({ estado: nuevoEstadoLead }).eq('id', matched.id);
          updated = true;
        }
      }
      // Fallback: match by nombre when instagram didn't find anything
      if (!updated && callData.nombre && callData.nombre !== 'Sin nombre') {
        const { data: byNombre } = await supabase
          .from('leads').select('id')
          .ilike('nombre', callData.nombre)
          .eq('cliente_id', req.cliente_id)
          .limit(1).maybeSingle();
        if (byNombre) {
          await supabase.from('leads').update({ estado: nuevoEstadoLead }).eq('id', byNombre.id);
        }
      }
    }

    // Persistir calificado=true en el lead cuando la call se marca como Calificada
    // El flag debe sobrevivir aunque el lead luego avance a Cerrado
    if (estado === 'Calificada') {
      let leadId = null;
      if (callData.instagram) {
        const { data: m } = await supabase.from('leads').select('id')
          .ilike('instagram', callData.instagram).eq('cliente_id', req.cliente_id)
          .limit(1).maybeSingle();
        if (m) leadId = m.id;
      }
      if (!leadId && callData.nombre && callData.nombre !== 'Sin nombre') {
        const { data: m } = await supabase.from('leads').select('id')
          .ilike('nombre', callData.nombre).eq('cliente_id', req.cliente_id)
          .limit(1).maybeSingle();
        if (m) leadId = m.id;
      }
      if (leadId) {
        await supabase.from('leads').update({ calificado: true }).eq('id', leadId);
      }
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 DELETE CALL
// ===============================
app.delete('/call/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('calls')
      .delete()
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ DELETE CALL:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 CREATE LEAD
// ===============================
app.post('/lead', validateAccess, async (req, res) => {
  console.log(`[/lead] body: ${JSON.stringify(req.body)}`);
  try {
    // GHL Custom Webhook sends fields inside customData — fall back to top-level for direct calls
    const src = req.body?.customData || req.body;
    console.log(`[/lead:1] src (customData o body): ${JSON.stringify(src)}`);
    const { nombre, instagram: instagramRaw, mensaje, origen, tipo, etiqueta } = src;
    console.log(`[/lead:2] instagramRaw="${instagramRaw}" nombre="${nombre}" etiqueta="${etiqueta}" origen="${origen}"`);

    // Normalize instagram: strip leading @, lowercase
    const instagram = (instagramRaw || '').replace(/^@/, '').toLowerCase().trim() || null;
    console.log(`[/lead:3] instagram normalizado="${instagram}"`);

    // Reject GHL placeholder values sent when the custom field is empty
    const INVALID_IG = new Set(['-', '--', 'n/a', 'na', 'none', 'null', 'undefined', 'sin instagram']);
    if (!instagram || INVALID_IG.has(instagram)) {
      console.log(`[/lead:4] BLOQUEADO por INVALID_IG → instagram="${instagram}" → 400`);
      return res.status(400).json({ error: 'Falta instagram' });
    }
    console.log(`[/lead:4] instagram válido, continúa`);

    // nombre: customData.nombre → req.body.full_name → req.body.first_name → 'Sin nombre'
    const rawNombre = nombre
      || req.body.full_name
      || [req.body.first_name, req.body.last_name].filter(Boolean).join(' ').trim()
      || '';
    const nombreLimpio = rawNombre && !rawNombre.includes('{{') ? rawNombre : 'Sin nombre';

    const tipoFinal = tipo || 'comentario';
    const ALLOWED_ORIGEN = ['Inbound', 'Outbound'];
    const origenFinal = ALLOWED_ORIGEN.includes(origen) ? origen : 'Inbound';
    const etiquetaFinal = etiqueta || '';
    const tipoLead = 'Organico';
    const now = new Date().toISOString();
    console.log(`[/lead:5] nombreLimpio="${nombreLimpio}" etiquetaFinal="${etiquetaFinal}" cliente_id="${req.cliente_id}"`);

    // Check if lead already exists for this client
    console.log(`[/lead:6] buscando duplicado → instagram="${instagram}" cliente_id="${req.cliente_id}"`);
    const { data: existingArr, error: searchError } = await supabase
      .from('leads')
      .select('id, etiquetas, etiqueta')
      .eq('instagram', instagram)
      .eq('cliente_id', req.cliente_id)
      .limit(1);

    if (searchError) console.error(`[/lead:6] error búsqueda duplicado:`, searchError);
    let existing = existingArr?.[0] || null;
    console.log(`[/lead:7] duplicado encontrado: ${existing ? `id=${existing.id}` : 'NO (es lead nuevo)'}`);

    // Secondary dedup by nombre if no instagram match
    if (!existing && nombreLimpio && nombreLimpio !== 'Sin nombre') {
      console.log(`[/lead:6b] sin match por instagram, buscando por nombre → "${nombreLimpio}"`);
      const { data: nameArr } = await supabase
        .from('leads')
        .select('id, etiquetas, etiqueta')
        .ilike('nombre', nombreLimpio.trim())
        .eq('cliente_id', req.cliente_id)
        .limit(1);
      if (nameArr?.[0]) {
        existing = nameArr[0];
        console.log(`[/lead:7b] match por nombre → id=${existing.id}, se actualizará etiqueta`);
      }
    }

    if (existing) {
      // Append new etiqueta to array — never overwrite
      const prev = Array.isArray(existing.etiquetas) && existing.etiquetas.length
        ? existing.etiquetas
        : (existing.etiqueta ? [existing.etiqueta] : []);
      const newEtiquetas = etiquetaFinal && !prev.includes(etiquetaFinal) ? [...prev, etiquetaFinal] : prev;

      console.log(`[/lead:8] UPDATE existente id=${existing.id} newEtiquetas=${JSON.stringify(newEtiquetas)}`);
      const { error: updateError } = await supabase
        .from('leads')
        .update({ etiquetas: newEtiquetas, ultima_accion: mensaje || '', updated_at: now })
        .eq('id', existing.id)
        .eq('cliente_id', req.cliente_id);

      if (updateError) {
        console.error('❌ UPDATE LEAD (webhook):', updateError);
        return res.status(500).json({ error: updateError.message });
      }
      console.log(`[/lead:8] UPDATE OK`);
    } else {
      // New lead
      console.log(`[/lead:9] INSERT nuevo lead instagram="${instagram}"`);
      const { error: insertError } = await supabase
        .from('leads')
        .insert({
          nombre: nombreLimpio,
          instagram,
          ultima_accion: mensaje || '',
          origen: origenFinal,
          tipo: tipoLead,
          estado: 'Primer contacto',
          etiqueta: etiquetaFinal,
          etiquetas: etiquetaFinal ? [etiquetaFinal] : [],
          source: 'manychat',
          cliente_id: req.cliente_id,
          created_at: now,
          updated_at: now,
        });

      if (insertError) {
        console.error('❌ INSERT LEAD (webhook):', insertError);
        return res.status(500).json({ error: insertError.message });
      }
      console.log(`[/lead:9] INSERT OK`);
    }

    await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiquetaFinal || 'desconocido',
        tipo: tipoFinal,
        cliente_id: req.cliente_id
      });

    console.log(`[/lead:10] FINALIZADO → 200 ok`);
    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER /lead:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 GET CLIENTES
// ===============================
app.get('/clientes', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('clientes')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('❌ GET CLIENTES:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 CREATE CLIENTE
// ===============================
app.post('/clientes', validateAccess, async (req, res) => {
  try {
    const {
      nombre, instagram, inicio, fin, tipo_pago, cash_collected,
      comprobante, estado, pp, proxpaso, road, mod, proxpago, programa, origen
    } = req.body;

    if (!nombre) {
      return res.status(400).json({ error: 'nombre es obligatorio' });
    }

    // Evitar duplicado por instagram
    if (instagram) {
      const { data: existe } = await supabase
        .from('clientes')
        .select('id')
        .eq('cliente_id', req.cliente_id)
        .eq('instagram', instagram.toLowerCase())
        .maybeSingle();

      if (existe) {
        return res.status(409).json({ error: 'Ya existe cliente con ese instagram' });
      }
    }

    const { data, error } = await supabase
      .from('clientes')
      .insert([{
        cliente_id: req.cliente_id,
        nombre,
        instagram: instagram ? instagram.toLowerCase() : null,
        inicio: inicio || null,
        fin: fin || null,
        tipo_pago: tipo_pago || 'Contado',
        cash_collected: cash_collected || 0,
        comprobante: comprobante || '',
        estado: estado || 'Al día',
        pp: pp || null,
        proxpaso: proxpaso || null,
        road: road || null,
        mod: mod || null,
        proxpago: proxpago || null,
        programa: programa || null,
        origen: origen || null,
      }])
      .select()
      .single();

    if (error) {
      console.error('❌ CREATE CLIENTE:', error);
      return res.status(500).json({ error: error.message });
    }

    // Auto-crear alumno vinculado al cliente
    try {
      const parts = (nombre || '').trim().split(' ');
      await supabase.from('alumnos').insert([{
        cliente_id: req.cliente_id,
        nombre: parts[0] || nombre,
        apellido: parts.slice(1).join(' ') || '',
        negocio: programa || '',
        instagram: instagram ? instagram.toLowerCase().replace(/^@/, '') : '',
        source_id: data.id
      }]);
    } catch (e) { /* no bloquear si falla */ }

    res.json(data);

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 UPDATE CLIENTE
// ===============================
app.patch('/clientes/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    const updates = { ...req.body };

    delete updates.id;
    delete updates.cliente_id;
    delete updates.created_at;

    if (updates.instagram) {
      updates.instagram = updates.instagram.toLowerCase();
    }

    const { error } = await supabase
      .from('clientes')
      .update(updates)
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ UPDATE CLIENTE:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 DELETE CLIENTE
// ===============================
app.delete('/clientes/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('clientes')
      .delete()
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);

    if (error) {
      console.error('❌ DELETE CLIENTE:', error);
      return res.status(500).json({ error: error.message });
    }

    // Limpiar cuotas huérfanas
    await supabase
      .from('cuotas_clientes')
      .delete()
      .eq('ref_cliente_id', id)
      .eq('cliente_id', req.cliente_id);

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// CUOTAS_CLIENTES CRUD
// ===============================
app.get('/cuotas', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('cuotas_clientes')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('fecha', { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({
      id: r.id,
      clienteId: r.ref_cliente_id,
      clienteNombre: r.cliente_nombre,
      clienteIg: r.cliente_ig,
      numero: r.numero,
      fecha: r.fecha,
      monto: r.monto,
      pagado: r.pagado,
      cash_collected: r.cash_collected,
    })));
  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

app.post('/cuotas', validateAccess, async (req, res) => {
  try {
    const { id, ref_cliente_id, cliente_nombre, cliente_ig, numero, fecha, monto, pagado, cash_collected } = req.body;
    const row = {
      cliente_id: req.cliente_id,
      ref_cliente_id: ref_cliente_id || null,
      cliente_nombre: cliente_nombre || null,
      cliente_ig: cliente_ig || null,
      numero: numero ?? 2,
      fecha: fecha || null,
      monto: monto ?? 0,
      pagado: pagado ?? false,
      cash_collected: cash_collected ?? 0,
    };
    if (id) row.id = id;
    const { data, error } = await supabase.from('cuotas_clientes').insert([row]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({
      id: data.id,
      clienteId: data.ref_cliente_id,
      clienteNombre: data.cliente_nombre,
      clienteIg: data.cliente_ig,
      numero: data.numero,
      fecha: data.fecha,
      monto: data.monto,
      pagado: data.pagado,
      cash_collected: data.cash_collected,
    });
  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

app.patch('/cuotas/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;
    const campos = {};
    if (req.body.pagado !== undefined) campos.pagado = req.body.pagado;
    if (req.body.monto !== undefined) campos.monto = req.body.monto;
    if (req.body.cash_collected !== undefined) campos.cash_collected = req.body.cash_collected;
    if (req.body.fecha !== undefined) campos.fecha = req.body.fecha;
    if (!Object.keys(campos).length) return res.status(400).json({ error: 'Sin campos' });
    const { error } = await supabase
      .from('cuotas_clientes')
      .update(campos)
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

app.delete('/cuotas/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;
    const { error } = await supabase
      .from('cuotas_clientes')
      .delete()
      .eq('id', id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 GET /metrics?range=day|week|month
// ===============================
app.get('/metrics', async (req, res) => {
  try {
    // ── Auth ──
    const access = await checkAccess(req);
    if (!access.ok) return res.status(access.status).json({ error: access.error });
    const cliente_id = access.cliente_id;

    // ── Validación de range ──
    const { range = 'month' } = req.query;
    if (!['day', 'week', 'month'].includes(range)) {
      return res.status(400).json({ error: "range inválido. Usar: 'day', 'week' o 'month'" });
    }

    // ── Fechas en UTC puro ──
    const now = new Date();
    const todayUTC = new Date(Date.UTC(
      now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()
    ));

    let curStart, prevStart, prevEnd;

    if (range === 'day') {
      curStart  = new Date(todayUTC);
      prevEnd   = new Date(curStart);
      prevStart = new Date(curStart); prevStart.setUTCDate(prevStart.getUTCDate() - 1);
    } else if (range === 'week') {
      curStart  = new Date(todayUTC); curStart.setUTCDate(todayUTC.getUTCDate() - 7);
      prevEnd   = new Date(curStart);
      prevStart = new Date(curStart); prevStart.setUTCDate(prevStart.getUTCDate() - 7);
    } else {
      // range='month' → mes calendario actual vs mes anterior
      const yr = todayUTC.getUTCFullYear(), m = todayUTC.getUTCMonth();
      curStart  = new Date(Date.UTC(yr, m, 1));
      prevEnd   = new Date(Date.UTC(yr, m, 1));
      prevStart = new Date(Date.UTC(yr, m - 1, 1));
    }

    const curStartISO  = curStart.toISOString();
    const prevStartISO = prevStart.toISOString();
    const prevEndISO   = prevEnd.toISOString();

    // ── Queries en paralelo ──
    const [
      leadsNowRes,
      leadsPrevRes,
      callsNowRes,
      callsPrevRes,
      clientesNowRes,
      clientesPrevRes,
    ] = await Promise.all([
      supabase.from('leads').select('id,estado').eq('cliente_id', cliente_id).gte('created_at', curStartISO),
      supabase.from('leads').select('id,estado').eq('cliente_id', cliente_id).gte('created_at', prevStartISO).lt('created_at', prevEndISO),
      supabase.from('calls').select('id,estado').eq('cliente_id', cliente_id).gte('created_at', curStartISO),
      supabase.from('calls').select('id,estado').eq('cliente_id', cliente_id).gte('created_at', prevStartISO).lt('created_at', prevEndISO),
      supabase.from('clientes').select('id,cash_collected').eq('cliente_id', cliente_id).gte('created_at', curStartISO),
      supabase.from('clientes').select('id,cash_collected').eq('cliente_id', cliente_id).gte('created_at', prevStartISO).lt('created_at', prevEndISO),
    ]);

    // ── Validación individual por query ──
    const queries = {
      'leads (actual)':      leadsNowRes,
      'leads (anterior)':    leadsPrevRes,
      'calls (actual)':      callsNowRes,
      'calls (anterior)':    callsPrevRes,
      'clientes (actual)':   clientesNowRes,
      'clientes (anterior)': clientesPrevRes,
    };
    for (const [name, result] of Object.entries(queries)) {
      if (result.error) {
        console.error(`❌ /metrics query "${name}":`, result.error);
        return res.status(500).json({ error: `Error en query "${name}": ${result.error.message}` });
      }
    }

    // ── Cálculo de métricas ──
    const calc = (leads, calls, clientes) => {
      const l  = leads    || [];
      const c  = calls    || [];
      const cl = clientes || [];

      const totalLeads     = l.length;
      const closes         = l.filter(x => x.estado === 'Cerrado' || x.estado === 'Seña').length;
      const senas          = l.filter(x => x.estado === 'Seña').length;
      const totalCalls     = c.length;
      const shows          = c.filter(x => !['No asistió','Cancelada','Re agenda','Pendiente'].includes(x.estado)).length;
      const facturacion    = cl.reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0);
      const cash_collected = facturacion;
      const aov            = closes > 0 ? Math.round(facturacion / closes) : 0;

      return { leads: totalLeads, calls: totalCalls, shows, closes, senas, facturacion, cash_collected, aov };
    };

    res.json({
      range,
      current:  calc(leadsNowRes.data,  callsNowRes.data,  clientesNowRes.data),
      previous: calc(leadsPrevRes.data, callsPrevRes.data, clientesPrevRes.data),
    });

  } catch (err) {
    console.error('❌ GET /metrics:', err);
    res.status(500).json({ error: err.message });
  }
});


// ===============================
// 🔥 INGRESOS
// ===============================
app.get('/ingresos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ingresos').select('*')
      .eq('cliente_id', req.cliente_id).order('fecha', { ascending: false, nullsFirst: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data.map(r => ({
      id: r.id, concepto: r.concepto, tipo: r.tipo, tipoPago: r.tipo_pago,
      nombre: r.nombre, usd: r.usd || 0, ars: r.ars || 0, eur: r.eur || 0,
      fecha: r.fecha, origen: r.origen, instagram: r.instagram,
      cuotaId: r.cuota_id, clienteId: r.ref_cliente_id, clienteNombre: r.cliente_nombre,
    })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/ingresos', validateAccess, async (req, res) => {
  try {
    const b = req.body;
    const { data, error } = await supabase.from('ingresos').insert([{
      cliente_id: req.cliente_id,
      concepto: b.concepto || null, tipo: b.tipo || null,
      tipo_pago: b.tipoPago || b.tipo_pago || null, nombre: b.nombre || null,
      usd: +b.usd || 0, ars: +b.ars || 0, eur: +b.eur || 0, fecha: b.fecha || null,
      origen: b.origen || null,
      instagram: b.instagram ? b.instagram.toLowerCase() : null,
      cuota_id: b.cuotaId || b.cuota_id || null,
      ref_cliente_id: b.clienteId || b.ref_cliente_id || null,
      cliente_nombre: b.clienteNombre || b.cliente_nombre || null,
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...b, id: data.id });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/ingresos/:id', validateAccess, async (req, res) => {
  try {
    const b = req.body;
    const updates = {};
    if (b.nombre !== undefined) updates.nombre = b.nombre;
    if (b.usd !== undefined) updates.usd = +b.usd;
    const { error } = await supabase.from('ingresos').update(updates)
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/ingresos/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('ingresos').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🔥 EGRESOS
// ===============================
app.get('/egresos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('egresos').select('*')
      .eq('cliente_id', req.cliente_id).order('fecha', { ascending: false, nullsFirst: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/egresos', validateAccess, async (req, res) => {
  try {
    const b = req.body;
    const { data, error } = await supabase.from('egresos').insert([{
      cliente_id: req.cliente_id,
      concepto: b.concepto || null, tipo: b.tipo || null, cat: b.cat || null,
      usd: +b.usd || 0, ars: +b.ars || 0, eur: +b.eur || 0, fecha: b.fecha || null,
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...b, id: data.id });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/egresos/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('egresos').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ========== APODOS DE USUARIOS Y ALIASES DE CLIENTES ==========
// Sin middleware validateAccess: solo requieren x-user-email válido

app.get('/user-nicknames', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!email) return res.status(400).json({ error: 'Falta x-user-email' });
    const { data, error } = await supabase.from('user_nicknames').select('user_email,nickname');
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/user-nicknames', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!email) return res.status(400).json({ error: 'Falta x-user-email' });
    const { nickname } = req.body;
    if (!nickname?.trim()) return res.status(400).json({ error: 'El apodo no puede estar vacío' });
    const { error } = await supabase.from('user_nicknames')
      .upsert({ user_email: email, nickname: nickname.trim(), updated_at: new Date().toISOString() }, { onConflict: 'user_email' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/client-aliases', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!email) return res.status(400).json({ error: 'Falta x-user-email' });
    const { data, error } = await supabase.from('client_aliases').select('cliente_id,alias');
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/client-aliases/can-edit', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!email) return res.json({ canEdit: false });
    const canEdit = await holdingAccess(email);
    res.json({ canEdit: !!canEdit });
  } catch (err) { res.json({ canEdit: false }); }
});

app.post('/client-aliases', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!email) return res.status(400).json({ error: 'Falta x-user-email' });
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Solo el super-admin puede cambiar los apodos de clientes' });
    const { cliente_id, alias } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'Falta cliente_id' });
    if (!alias?.trim()) {
      await supabase.from('client_aliases').delete().eq('cliente_id', cliente_id);
    } else {
      const { error } = await supabase.from('client_aliases')
        .upsert({ cliente_id, alias: alias.trim(), updated_at: new Date().toISOString() }, { onConflict: 'cliente_id' });
      if (error) return res.status(500).json({ error: error.message });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ===============================
// 🔥 ACTIVITY LOG
// ===============================
app.get('/activity', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('activity_log').select('*')
      .eq('cliente_id', req.cliente_id).order('created_at', { ascending: false }).limit(100);
    if (error) return res.status(500).json({ error: error.message });
    res.json(data.map(r => ({
      id: r.id, ts: r.ts_iso || r.created_at,
      leadId: r.lead_id || '', nombre: r.lead_nombre || '—',
      instagram: r.lead_instagram || '', accion: r.accion || '',
      detalle: r.detalle || '', usuario: r.usuario || '',
    })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/activity', validateAccess, async (req, res) => {
  try {
    const { accion, lead_nombre, lead_instagram, detalle, usuario, lead_id, ts_iso } = req.body;
    const { error } = await supabase.from('activity_log').insert([{
      cliente_id: req.cliente_id, accion: accion || '',
      lead_nombre: lead_nombre || '', lead_instagram: lead_instagram || '',
      detalle: detalle || '', usuario: usuario || '', lead_id: lead_id || '',
      ts_iso: ts_iso || new Date().toISOString(),
    }]);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/activity/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('activity_log').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🏋 EQUIPO MEMBERS
// ===============================
app.get('/equipo/members', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('equipo_members')
      .select('*').eq('cliente_id', req.cliente_id).order('created_at');
    if (error) return res.status(500).json({ error: error.message });
    const result = (data || []).map(m => {
      const meta = (m.rules || []).find(r => r.id === '_agendas');
      return { ...m, agendas_manual: meta ? meta.count : null, rules: (m.rules || []).filter(r => r.id !== '_agendas') };
    });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/equipo/members', validateAccess, async (req, res) => {
  try {
    const { nombre, role } = req.body;
    if (!nombre || !role) return res.status(400).json({ error: 'nombre y role son obligatorios' });
    const { data, error } = await supabase.from('equipo_members')
      .insert([{ cliente_id: req.cliente_id, nombre, role, rules: [] }])
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/equipo/members/:id', validateAccess, async (req, res) => {
  try {
    const updates = {};
    if (req.body.nombre !== undefined) updates.nombre = req.body.nombre;

    // agendas_manual stored inside rules array as { id:'_agendas', count:N }
    if (req.body.agendas_manual !== undefined || req.body.rules !== undefined) {
      const { data: current } = await supabase.from('equipo_members').select('rules').eq('id', req.params.id).eq('cliente_id', req.cliente_id).single();
      let rules = Array.isArray(current?.rules) ? current.rules.filter(r => r.id !== '_agendas') : [];
      if (req.body.rules !== undefined) rules = req.body.rules.filter(r => r.id !== '_agendas');
      if (req.body.agendas_manual !== undefined && req.body.agendas_manual !== null) {
        rules = [...rules, { id: '_agendas', count: Number(req.body.agendas_manual) }];
      }
      updates.rules = rules;
    }

    const { data, error } = await supabase.from('equipo_members')
      .update(updates).eq('id', req.params.id).eq('cliente_id', req.cliente_id)
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    const meta = (data.rules || []).find(r => r.id === '_agendas');
    data.agendas_manual = meta ? meta.count : null;
    data.rules = (data.rules || []).filter(r => r.id !== '_agendas');
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/equipo/members/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('equipo_members')
      .delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ===============================
// 👥 TEAM PRESENCE
// ===============================
app.post('/team/heartbeat', validateAccess, async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    const rol = req.user.role || req.body.rol || 'setter';
    const nombre = req.body.nombre || email.split('@')[0];
    const { error } = await supabase.from('team_presence').upsert({
      cliente_id: req.cliente_id,
      email,
      rol,
      nombre,
      last_seen: new Date().toISOString()
    }, { onConflict: 'cliente_id,email' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/team', validateAccess, async (req, res) => {
  try {
    const since = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { data, error } = await supabase.from('team_presence')
      .select('email, rol, nombre, last_seen')
      .eq('cliente_id', req.cliente_id)
      .gte('last_seen', since);
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ===============================
// 🎓 ALUMNOS
// ===============================
app.get('/alumnos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('alumnos').select('*')
      .eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/alumnos', validateAccess, async (req, res) => {
  try {
    const { nombre, apellido, negocio, email, instagram } = req.body;
    if (!nombre) return res.status(400).json({ error: 'Falta nombre' });
    const { data, error } = await supabase.from('alumnos').insert([{
      cliente_id: req.cliente_id,
      nombre: nombre.trim(),
      apellido: (apellido || '').trim(),
      negocio: (negocio || '').trim(),
      email: (email || '').trim(),
      instagram: instagram ? instagram.toLowerCase().replace(/^@/, '') : '',
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/alumnos/:id', validateAccess, async (req, res) => {
  try {
    const updates = { ...req.body };
    delete updates.id; delete updates.cliente_id; delete updates.created_at;
    if (updates.instagram !== undefined) {
      updates.instagram = (updates.instagram || '').toLowerCase().replace(/^@+/, '').trim();
    }
    const { data, error } = await supabase.from('alumnos').update(updates)
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/alumnos/:id', validateAccess, async (req, res) => {
  try {
    // Delete related records first to avoid FK constraint failures
    await supabase.from('reportes_semanales').delete()
      .eq('alumno_id', req.params.id).eq('cliente_id', req.cliente_id);
    await supabase.from('sesiones').delete()
      .eq('alumno_id', req.params.id).eq('cliente_id', req.cliente_id);
    const { error } = await supabase.from('alumnos').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Endpoint público: solo nombre/apellido de un alumno (para pre-llenar el formulario)
app.get('/alumno/:id', async (req, res) => {
  try {
    const { data, error } = await supabase.from('alumnos')
      .select('nombre,apellido,negocio').eq('id', req.params.id).single();
    if (error || !data) return res.status(404).json({ error: 'No encontrado' });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 📋 REPORTES SEMANALES
// ===============================
app.get('/reportes', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('reportes_semanales').select('*')
      .eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Helper: límites de semana actual (Lun 00:00 – Dom 23:59 UTC) ──
function _currentWeekBounds() {
  const now = new Date();
  const dow = now.getUTCDay(); // 0=Dom
  const daysToMon = dow === 0 ? 6 : dow - 1;
  const mon = new Date(now); mon.setUTCDate(now.getUTCDate() - daysToMon); mon.setUTCHours(0, 0, 0, 0);
  const sun = new Date(mon); sun.setUTCDate(mon.getUTCDate() + 6); sun.setUTCHours(23, 59, 59, 999);
  return { start: mon.toISOString(), end: sun.toISOString() };
}

// ── Público: check estado del reporte de la semana actual para un alumno ──
app.get('/reportes/check/:alumno_id', async (req, res) => {
  try {
    const { alumno_id } = req.params;
    const { cliente_id } = req.query;
    if (!alumno_id || !cliente_id) return res.status(400).json({ error: 'Faltan parámetros' });
    const { start, end } = _currentWeekBounds();
    const { data: rep } = await supabase
      .from('reportes_semanales')
      .select('*')
      .eq('alumno_id', alumno_id)
      .eq('cliente_id', cliente_id)
      .gte('submitted_at', start)
      .lte('submitted_at', end)
      .order('submitted_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (!rep) return res.json({ hasReport: false });
    const now = new Date();
    const editable = rep.editable_until ? new Date(rep.editable_until) > now : false;
    const locked = rep.locked && !editable;
    // Check if there's a pending edit request
    const { data: req_ } = await supabase
      .from('weekly_report_edit_requests')
      .select('id,estado')
      .eq('reporte_id', rep.id)
      .in('estado', ['pending'])
      .limit(1)
      .maybeSingle();
    res.json({ hasReport: true, reporte_id: rep.id, locked, editable, editable_until: rep.editable_until, report: rep, pendingRequest: !!req_ });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Público: alumnos envían su reporte sin necesitar auth
app.post('/reportes', async (req, res) => {
  try {
    let { cliente_id, alumno_id, nombre, apellido, instagram, semana, estado,
            situacion, objetivos, logros, problemas, ayuda,
            implementacion, porque_no, extra, respuestas } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'Falta cliente_id' });

    // New dynamic form sends respuestas JSONB; extract legacy fields for backward compat
    if (respuestas && typeof respuestas === 'object') {
      const s = v => (Array.isArray(v) ? v.join(', ') : (v != null ? String(v) : ''));
      if (respuestas.q1 !== undefined) estado        = s(respuestas.q1);
      if (respuestas.q2 !== undefined) situacion     = s(respuestas.q2);
      if (respuestas.q3 !== undefined) objetivos     = s(respuestas.q3);
      if (respuestas.q4 !== undefined) logros        = s(respuestas.q4);
      if (respuestas.q5 !== undefined) problemas     = s(respuestas.q5);
      if (respuestas.q6 !== undefined) ayuda         = Array.isArray(respuestas.q6) ? respuestas.q6 : (respuestas.q6 ? [respuestas.q6] : []);
      if (respuestas.q7 !== undefined) implementacion= s(respuestas.q7);
      if (respuestas.q8 !== undefined) extra         = s(respuestas.q8);
      if (respuestas.porque_no !== undefined) porque_no = s(respuestas.porque_no);
    }
    const { data: check } = await supabase.from('user_clientes')
      .select('cliente_id').eq('cliente_id', cliente_id).limit(1);
    if (!check || check.length === 0) return res.status(400).json({ error: 'Cliente inválido' });

    // Auto-asignar alumno por instagram si no viene alumno_id
    const igClean = instagram ? instagram.toLowerCase().replace(/^@+/, '').trim() : '';
    if (!alumno_id && igClean) {
      const { data: matchDirect } = await supabase.from('alumnos')
        .select('id').eq('cliente_id', cliente_id).eq('instagram', igClean).maybeSingle();
      if (matchDirect) {
        alumno_id = matchDirect.id;
      } else {
        const { data: matchCliente } = await supabase.from('clientes')
          .select('id').eq('cliente_id', cliente_id).eq('instagram', igClean).maybeSingle();
        if (matchCliente) {
          const { data: matchAlumno } = await supabase.from('alumnos')
            .select('id').eq('cliente_id', cliente_id).eq('source_id', matchCliente.id).maybeSingle();
          if (matchAlumno) alumno_id = matchAlumno.id;
        }
      }
    }

    // ── Bloqueo por período semanal ──
    if (alumno_id) {
      const { start, end } = _currentWeekBounds();
      const { data: existing } = await supabase
        .from('reportes_semanales')
        .select('id, locked, editable_until')
        .eq('alumno_id', alumno_id)
        .eq('cliente_id', cliente_id)
        .gte('submitted_at', start)
        .lte('submitted_at', end)
        .limit(1)
        .maybeSingle();

      if (existing) {
        const now = new Date();
        const editableUntil = existing.editable_until ? new Date(existing.editable_until) : null;
        const canEdit = editableUntil && editableUntil > now;

        if (!canEdit) {
          // Reporte bloqueado
          return res.status(409).json({ error: 'Ya enviaste el reporte de esta semana', locked: true, reporte_id: existing.id });
        }

        // Edición temporal aprobada — actualizar el reporte existente
        const fields = { situacion, objetivos, logros, problemas, ayuda: ayuda || [], implementacion, porque_no: porque_no || '', extra: extra || '', estado: estado || '', semana: semana || '', respuestas: respuestas || null, locked: true, editable_until: null };
        const { data: updated, error: updErr } = await supabase
          .from('reportes_semanales').update(fields).eq('id', existing.id).select().single();
        if (updErr) return res.status(500).json({ error: updErr.message });
        _discordNotify('report_submitted', { alumno_id, cliente_id });
        return res.json({ ...updated, updated: true });
      }
    }

    const now = new Date();
    const { data, error } = await supabase.from('reportes_semanales').insert([{
      cliente_id,
      alumno_id: alumno_id || null,
      nombre: nombre || '',
      apellido: apellido || '',
      instagram: igClean || (instagram ? instagram.toLowerCase().replace(/^@/, '') : ''),
      semana: semana || '',
      estado: estado || '',
      situacion: situacion || '',
      objetivos: objetivos || '',
      logros: logros || '',
      problemas: problemas || '',
      ayuda: ayuda || [],
      implementacion: implementacion || '',
      porque_no: porque_no || '',
      extra: extra || '',
      respuestas: respuestas || null,
      submitted_at: now.toISOString(),
      locked: true,
      editable_until: null,
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    _discordNotify('report_submitted', { alumno_id, cliente_id });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Público: alumno solicita edición de su reporte ──
app.post('/reportes/:id/request-edit', async (req, res) => {
  try {
    const { alumno_id, cliente_id, motivo } = req.body;
    if (!alumno_id || !cliente_id || !motivo?.trim()) return res.status(400).json({ error: 'Faltan campos requeridos' });
    // Verificar que el reporte pertenece al alumno
    const { data: rep } = await supabase.from('reportes_semanales')
      .select('id').eq('id', req.params.id).eq('alumno_id', alumno_id).eq('cliente_id', cliente_id).maybeSingle();
    if (!rep) return res.status(404).json({ error: 'Reporte no encontrado' });
    // Evitar solicitudes duplicadas pendientes
    const { data: existing } = await supabase.from('weekly_report_edit_requests')
      .select('id').eq('reporte_id', req.params.id).eq('estado', 'pending').maybeSingle();
    if (existing) return res.status(409).json({ error: 'Ya tenés una solicitud pendiente para este reporte' });
    const { data, error } = await supabase.from('weekly_report_edit_requests').insert([{
      reporte_id: req.params.id,
      alumno_id,
      cliente_id,
      motivo: motivo.trim(),
      estado: 'pending',
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    // Notify admin Discord channel if configured
    const { data: alumnoInfo } = await supabase.from('alumnos')
      .select('nombre, apellido').eq('id', alumno_id).maybeSingle();
    _discordNotify('edit_requested', { nombre: alumnoInfo?.nombre, apellido: alumnoInfo?.apellido, motivo: motivo.trim() });
    res.json({ ok: true, id: data.id });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Admin: ver solicitudes de edición ──
app.get('/reportes/edit-requests', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('weekly_report_edit_requests')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    // Join report info
    const reporteIds = [...new Set((data || []).map(r => r.reporte_id))];
    let reportesMap = {};
    if (reporteIds.length) {
      const { data: reps } = await supabase.from('reportes_semanales')
        .select('id,nombre,apellido,instagram,semana,alumno_id').in('id', reporteIds);
      (reps || []).forEach(r => { reportesMap[r.id] = r; });
    }
    res.json((data || []).map(r => ({ ...r, reporte: reportesMap[r.reporte_id] || null })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Admin: aprobar o rechazar solicitud de edición ──
app.patch('/reportes/edit-requests/:id', validateAccess, async (req, res) => {
  try {
    const { action } = req.body; // 'approve' | 'reject'
    if (!['approve', 'reject'].includes(action)) return res.status(400).json({ error: 'action debe ser approve o reject' });
    const { data: editReq } = await supabase.from('weekly_report_edit_requests')
      .select('*').eq('id', req.params.id).eq('cliente_id', req.cliente_id).maybeSingle();
    if (!editReq) return res.status(404).json({ error: 'Solicitud no encontrada' });
    const now = new Date();
    const updates = { estado: action === 'approve' ? 'approved' : 'rejected', approved_by: req.user?.user_email || null, approved_at: now.toISOString() };
    await supabase.from('weekly_report_edit_requests').update(updates).eq('id', req.params.id);
    if (action === 'approve') {
      const editableUntil = new Date(now.getTime() + 2 * 60 * 60 * 1000); // +2 horas
      await supabase.from('reportes_semanales')
        .update({ locked: false, editable_until: editableUntil.toISOString() })
        .eq('id', editReq.reporte_id);
      _discordNotify('edit_approved', { alumno_id: editReq.alumno_id, cliente_id: editReq.cliente_id });
    } else {
      _discordNotify('edit_rejected', { alumno_id: editReq.alumno_id });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/reportes/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('reportes_semanales').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🔄 SYNC CLIENTES → ALUMNOS
// ===============================
app.post('/sync-alumnos-from-clientes', validateAccess, async (req, res) => {
  try {
    const [{ data: clientes }, { data: alumnosExist }] = await Promise.all([
      supabase.from('clientes').select('id,nombre,programa,instagram').eq('cliente_id', req.cliente_id),
      supabase.from('alumnos').select('id,source_id').eq('cliente_id', req.cliente_id)
    ]);
    const activeSourceIds = new Set((clientes || []).map(c => c.id));
    const existingIds = new Set((alumnosExist || []).map(a => a.source_id).filter(Boolean));

    // Remove alumnos whose source client no longer exists
    const toRemove = (alumnosExist || []).filter(a => a.source_id && !activeSourceIds.has(a.source_id));
    for (const a of toRemove) {
      await supabase.from('reportes_semanales').delete().eq('alumno_id', a.id).eq('cliente_id', req.cliente_id);
      await supabase.from('sesiones').delete().eq('alumno_id', a.id).eq('cliente_id', req.cliente_id);
      await supabase.from('alumnos').delete().eq('id', a.id).eq('cliente_id', req.cliente_id);
    }

    // Add alumnos for new clientes
    const toCreate = (clientes || []).filter(c => !existingIds.has(c.id));
    let created = 0;
    for (const c of toCreate) {
      const parts = (c.nombre || '').trim().split(' ');
      await supabase.from('alumnos').insert([{
        cliente_id: req.cliente_id,
        nombre: parts[0] || c.nombre,
        apellido: parts.slice(1).join(' ') || '',
        negocio: c.programa || '',
        instagram: c.instagram ? c.instagram.toLowerCase().replace(/^@/, '') : '',
        source_id: c.id
      }]);
      created++;
    }
    res.json({ synced: created, removed: toRemove.length, total: clientes?.length || 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ===============================
// 📅 SESIONES
// ===============================
app.get('/sesiones', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('sesiones').select('*')
      .eq('cliente_id', req.cliente_id).order('fecha', { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/sesiones', validateAccess, async (req, res) => {
  try {
    const { alumno_id, fecha, hora, notas_previas } = req.body;
    if (!fecha) return res.status(400).json({ error: 'Falta fecha' });
    const { data, error } = await supabase.from('sesiones').insert([{
      cliente_id: req.cliente_id,
      alumno_id: alumno_id || null,
      fecha, hora: hora || null,
      notas_previas: notas_previas || '',
      resumen: ''
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);

    // Enviar email al alumno (no bloqueante)
    if (alumno_id) {
      supabase.from('alumnos').select('id,nombre,apellido,email').eq('id', alumno_id).single()
        .then(({ data: alumno }) => sendSessionEmail(alumno, data, req.cliente_id))
        .catch(err => console.error('Email sesión error:', err));
    }
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/sesiones/:id', validateAccess, async (req, res) => {
  try {
    const updates = { ...req.body };
    delete updates.id; delete updates.cliente_id; delete updates.created_at;
    const { data, error } = await supabase.from('sesiones').update(updates)
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/sesiones/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('sesiones').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🏢 HOLDING
// ===============================
async function holdingAccess(email) {
  if (!email) return false;
  const { data } = await supabase.from('user_clientes')
    .select('role').eq('user_email', email).eq('cliente_id', 'holding').maybeSingle();
  return !!data;
}

app.get('/holding/clientes', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { data } = await supabase.from('user_clientes').select('cliente_id')
      .eq('user_email', email).neq('cliente_id', 'holding');
    const unique = [...new Set((data || []).map(x => x.cliente_id))].sort();
    res.json(unique);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/holding/metrics', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });

    const ids = (req.query.cliente_ids || '').split(',').filter(Boolean);
    if (!ids.length) return res.json([]);

    const { from, to } = req.query;
    const year = parseInt(req.query.year) || new Date().getFullYear();

    // Fetch all holding_config percentages in one query
    const { data: allConfigs } = await supabase.from('holding_config').select('cliente_id,porcentaje').in('cliente_id', ids);
    const configMap = Object.fromEntries((allConfigs || []).map(c => [c.cliente_id, parseFloat(c.porcentaje) || 0]));

    // Fetch success cases count per client
    const { data: exitoRows } = await supabase.from('alumnos').select('cliente_id').in('cliente_id', ids).eq('es_caso_exito', true);
    const exitoMap = {};
    (exitoRows || []).forEach(r => { exitoMap[r.cliente_id] = (exitoMap[r.cliente_id] || 0) + 1; });

    const results = await Promise.all(ids.map(async cid => {
      // Facturación: suma de ingresos.usd filtrada por fecha
      let iq = supabase.from('ingresos').select('usd,fecha').eq('cliente_id', cid);
      if (from) iq = iq.gte('fecha', from);
      if (to)   iq = iq.lte('fecha', to);
      const { data: ingPeriod } = await iq;

      // Cash collected: suma de clientes.cash_collected filtrada por created_at
      let cq = supabase.from('clientes').select('cash_collected,created_at').eq('cliente_id', cid);
      if (from) cq = cq.gte('created_at', from + 'T00:00:00.000Z');
      if (to)   cq = cq.lte('created_at', to   + 'T23:59:59.999Z');
      const { data: cliPeriod } = await cq;

      // Gastos (egresos): suma de egresos.usd filtrada por fecha
      let gq = supabase.from('egresos').select('usd,fecha').eq('cliente_id', cid);
      if (from) gq = gq.gte('fecha', from);
      if (to)   gq = gq.lte('fecha', to);
      const { data: gasPeriod } = await gq;

      // Cierres: llamadas con estado Cierre
      let cq2 = supabase.from('calls').select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Cierre');
      if (from) cq2 = cq2.gte('created_at', from + 'T00:00:00.000Z');
      if (to)   cq2 = cq2.lte('created_at', to   + 'T23:59:59.999Z');
      const { data: callPeriod } = await cq2;

      // Agendas: leads con estado Agendado
      let aq = supabase.from('leads').select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Agendado');
      if (from) aq = aq.gte('created_at', from + 'T00:00:00.000Z');
      if (to)   aq = aq.lte('created_at', to   + 'T23:59:59.999Z');
      const { data: agPeriod } = await aq;

      // Datos anuales para gráfico de evolución mensual
      const { data: ingYear } = await supabase.from('ingresos')
        .select('usd,fecha').eq('cliente_id', cid)
        .gte('fecha', `${year}-01-01`)
        .lte('fecha', `${year}-12-31`);

      const { data: cliYear } = await supabase.from('clientes')
        .select('cash_collected,created_at').eq('cliente_id', cid)
        .gte('created_at', `${year}-01-01T00:00:00.000Z`)
        .lte('created_at', `${year}-12-31T23:59:59.999Z`);

      const { data: gasYear } = await supabase.from('egresos')
        .select('usd,fecha').eq('cliente_id', cid)
        .gte('fecha', `${year}-01-01`)
        .lte('fecha', `${year}-12-31`);

      const { data: callYear } = await supabase.from('calls')
        .select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Cierre')
        .gte('created_at', `${year}-01-01T00:00:00.000Z`)
        .lte('created_at', `${year}-12-31T23:59:59.999Z`);

      const { data: agYear } = await supabase.from('leads')
        .select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Agendado')
        .gte('created_at', `${year}-01-01T00:00:00.000Z`)
        .lte('created_at', `${year}-12-31T23:59:59.999Z`);

      const porcentaje = configMap[cid] || 0;

      const monthly = Array.from({ length: 12 }, (_, i) => {
        const m = String(i + 1).padStart(2, '0');
        const mIng  = (ingYear  || []).filter(x => (x.fecha || '').slice(5, 7) === m);
        const mCli  = (cliYear  || []).filter(x => (x.created_at || '').slice(5, 7) === m);
        const mGas  = (gasYear  || []).filter(x => (x.fecha || '').slice(5, 7) === m);
        const mCall = (callYear || []).filter(x => (x.created_at || '').slice(5, 7) === m);
        const mCC   = mCli.reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0);
        const mGasT = mGas.reduce((s, x) => s + (parseFloat(x.usd) || 0), 0);
        const mBal  = mCC - mGasT;
        const mFact = mIng.reduce((s, x) => s + (parseFloat(x.usd) || 0), 0);
        const mAg   = (agYear || []).filter(x => (x.created_at || '').slice(5, 7) === m).length;
        return {
          facturacion:     mFact,
          cash_collected:  mCC,
          gastos:          mGasT,
          balance_neto:    mBal,
          ingreso_holding: mBal * porcentaje / 100,
          closes:          mCall.length,
          agendas:         mAg,
          aov:             mCall.length > 0 ? Math.round(mFact / mCall.length) : 0
        };
      });

      const facturacion    = (ingPeriod  || []).reduce((s, x) => s + (parseFloat(x.usd)           || 0), 0);
      const cash_collected = (cliPeriod  || []).reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0);
      const gastos         = (gasPeriod  || []).reduce((s, x) => s + (parseFloat(x.usd)           || 0), 0);
      const balance_neto   = cash_collected - gastos;
      const ingreso_holding = balance_neto * porcentaje / 100;

      const closes  = (callPeriod || []).length;
      const agendas = (agPeriod  || []).length;
      const aov     = closes > 0 ? Math.round(facturacion / closes) : 0;

      return {
        cliente_id:     cid,
        facturacion,
        cash_collected,
        gastos,
        balance_neto,
        porcentaje,
        ingreso_holding,
        casos_exito:    exitoMap[cid] || 0,
        closes,
        agendas,
        aov,
        monthly
      };
    }));

    res.json(results);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🏢 HOLDING CONFIG (% por cliente)
// ===============================
app.get('/holding/config', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { data: userClients } = await supabase.from('user_clientes').select('cliente_id').eq('user_email', email).neq('cliente_id', 'holding');
    const ids = [...new Set((userClients || []).map(x => x.cliente_id))];
    if (!ids.length) return res.json([]);
    const { data } = await supabase.from('holding_config').select('*').in('cliente_id', ids);
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/holding/config/:cliente_id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { cliente_id } = req.params;
    const porcentaje = parseFloat(req.body.porcentaje);
    if (isNaN(porcentaje) || porcentaje < 0 || porcentaje > 100) return res.status(400).json({ error: 'Porcentaje inválido (0–100)' });
    const { error } = await supabase.from('holding_config').upsert({ cliente_id, porcentaje, updated_at: new Date().toISOString(), updated_by: email }, { onConflict: 'cliente_id' });
    if (error) return res.status(500).json({ error: error.message });
    console.log(`✅ Holding config → ${cliente_id}: ${porcentaje}%`);
    res.json({ ok: true, cliente_id, porcentaje });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 📸 HOLDING MONTHLY SNAPSHOTS
// ===============================
app.get('/holding/snapshots', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const ids = (req.query.cliente_ids || '').split(',').filter(Boolean);
    if (!ids.length) return res.json([]);
    const { data } = await supabase.from('holding_monthly_snapshots').select('*').in('cliente_id', ids).order('year', { ascending: false }).order('month', { ascending: false });
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/holding/snapshot', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { cliente_id, year, month, facturacion = 0, cash_collected = 0, gastos = 0, balance_neto = 0, porcentaje = 0, ingreso_holding = 0 } = req.body;
    if (!cliente_id || !year || !month) return res.status(400).json({ error: 'Faltan campos: cliente_id, year, month' });
    const { data, error } = await supabase.from('holding_monthly_snapshots').upsert({
      cliente_id, year: parseInt(year), month: parseInt(month),
      facturacion, cash_collected, gastos, balance_neto, porcentaje, ingreso_holding,
      snapshot_by: email, created_at: new Date().toISOString()
    }, { onConflict: 'cliente_id,year,month' }).select().single();
    if (error) return res.status(500).json({ error: error.message });
    console.log(`📸 Snapshot saved → ${cliente_id} ${year}/${month}`);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 🏆 HOLDING SUCCESS CASES
// ===============================
app.get('/holding/success-cases', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const ids = (req.query.cliente_ids || '').split(',').filter(Boolean);
    if (!ids.length) return res.json([]);
    const { data } = await supabase.from('alumnos').select('cliente_id,id,nombre,apellido,caso_exito_at').in('cliente_id', ids).eq('es_caso_exito', true).order('caso_exito_at', { ascending: false });
    const byClient = {};
    ids.forEach(id => { byClient[id] = []; });
    (data || []).forEach(a => { if (byClient[a.cliente_id]) byClient[a.cliente_id].push(a); });
    res.json(byClient);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 📸 AUTO-SNAPSHOT MES ANTERIOR
// ===============================
// Crea automáticamente snapshots del mes anterior si no existen.
// Se llama al abrir la tab Finanzas — corre silenciosamente.
app.post('/holding/auto-snapshot-previous', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });

    const ids = (req.query.cliente_ids || '').split(',').filter(Boolean);
    if (!ids.length) return res.json({ created: [], skipped: [], year: null, month: null });

    // Calcular mes anterior
    const now = new Date();
    const prevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const year  = prevMonth.getFullYear();
    const month = prevMonth.getMonth() + 1;
    const mStr  = String(month).padStart(2, '0');
    const from  = `${year}-${mStr}-01`;
    const lastDay = new Date(year, month, 0).getDate();
    const to    = `${year}-${mStr}-${String(lastDay).padStart(2, '0')}`;

    // Ver cuáles ya tienen snapshot para ese mes
    const { data: existing } = await supabase.from('holding_monthly_snapshots')
      .select('cliente_id').in('cliente_id', ids).eq('year', year).eq('month', month);
    const existingSet = new Set((existing || []).map(r => r.cliente_id));
    const toCreate = ids.filter(id => !existingSet.has(id));

    if (!toCreate.length) {
      return res.json({ created: [], skipped: [...existingSet], year, month });
    }

    // Cargar configs de porcentaje
    const { data: configs } = await supabase.from('holding_config')
      .select('cliente_id,porcentaje').in('cliente_id', toCreate);
    const cfgMap = Object.fromEntries((configs || []).map(c => [c.cliente_id, parseFloat(c.porcentaje) || 0]));

    const created = [];
    for (const cid of toCreate) {
      const [{ data: ing }, { data: cli }, { data: gas }] = await Promise.all([
        supabase.from('ingresos').select('usd').eq('cliente_id', cid).gte('fecha', from).lte('fecha', to),
        supabase.from('clientes').select('cash_collected').eq('cliente_id', cid)
          .gte('created_at', `${from}T00:00:00.000Z`).lte('created_at', `${to}T23:59:59.999Z`),
        supabase.from('egresos').select('usd').eq('cliente_id', cid).gte('fecha', from).lte('fecha', to),
      ]);
      const facturacion    = (ing  || []).reduce((s, x) => s + (parseFloat(x.usd)           || 0), 0);
      const cash_collected = (cli  || []).reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0);
      const gastos         = (gas  || []).reduce((s, x) => s + (parseFloat(x.usd)           || 0), 0);
      const balance_neto   = cash_collected - gastos;
      const porcentaje     = cfgMap[cid] || 0;
      const ingreso_holding = balance_neto * porcentaje / 100;

      const { error } = await supabase.from('holding_monthly_snapshots').upsert({
        cliente_id: cid, year, month,
        facturacion, cash_collected, gastos, balance_neto, porcentaje, ingreso_holding,
        snapshot_by: email + '[auto]', created_at: new Date().toISOString(),
      }, { onConflict: 'cliente_id,year,month' });

      if (!error) created.push(cid);
      else console.warn(`⚠ Auto-snapshot error ${cid}:`, error.message);
    }

    console.log(`📸 Auto-snapshot ${year}/${month}: creados=${created.length} skip=${existingSet.size}`);
    res.json({ created, skipped: [...existingSet], year, month });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 💹 HOLDING PCT (para CRM individual)
// ===============================
// Retorna el % de holding configurado para el cliente actual.
// Accesible por cualquier usuario autenticado del cliente.
app.get('/holding-pct', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase.from('holding_config')
      .select('porcentaje').eq('cliente_id', req.cliente_id).maybeSingle();
    res.json({ porcentaje: parseFloat(data?.porcentaje) || 0 });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ===============================
// 📋 SOPS
// ===============================
app.get('/sops', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('sops').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/sops', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('sops').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/sops/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('sops').update({ data: req.body }).eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/sops/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('sops').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Baúl de Ideas para contenido ──
app.get('/ideas', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ideas_contenido')
      .select('*').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/ideas', validateAccess, async (req, res) => {
  try {
    const { idea, motivo, area } = req.body;
    if (!idea?.trim()) return res.status(400).json({ error: 'La idea es obligatoria' });
    if (!['Marketing', 'Ventas', 'Producto'].includes(area)) return res.status(400).json({ error: 'Área inválida' });
    const { data, error } = await supabase.from('ideas_contenido')
      .insert([{ cliente_id: req.cliente_id, idea: idea.trim(), motivo: (motivo||'').trim(), area }])
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/ideas/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('ideas_contenido')
      .delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 🏛 FUNDACIONES
// ===============================
app.get('/fundaciones', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase.from('fundaciones').select('data').eq('cliente_id', req.cliente_id).single();
    res.json(data?.data || {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.put('/fundaciones', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('fundaciones').upsert({ cliente_id: req.cliente_id, data: req.body, updated_at: new Date().toISOString() }, { onConflict: 'cliente_id' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📝 CONTENIDO (posts + historias)
// ===============================
app.get('/contenido', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('contenido').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/contenido', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('contenido').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/contenido/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('contenido').update({ data: req.body }).eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: req.params.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/contenido/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('contenido').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 🎯 ANGULOS
// ===============================
app.get('/angulos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('angulos').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/angulos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('angulos').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/angulos/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('angulos').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 👥 REFERENTES
// ===============================
app.get('/referentes', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('referentes').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/referentes', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('referentes').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/referentes/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('referentes').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📊 METRICAS
// ===============================
app.get('/metricas', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('metricas').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/metricas', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('metricas').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/metricas/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('metricas').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📸 INSTAGRAM
// ===============================
app.get('/ig/cuenta', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase.from('ig_cuenta').select('data').eq('cliente_id', req.cliente_id).single();
    res.json(data?.data || {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.put('/ig/cuenta', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('ig_cuenta').upsert({ cliente_id: req.cliente_id, data: req.body, updated_at: new Date().toISOString() }, { onConflict: 'cliente_id' });
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/ig/reels', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ig_reels').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/ig/reels', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ig_reels').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/ig/reels/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('ig_reels').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/ig/carruseles', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ig_carruseles').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/ig/carruseles', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('ig_carruseles').insert({ cliente_id: req.cliente_id, data: req.body }).select('id').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/ig/carruseles/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('ig_carruseles').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 👥 HOLDING USERS
// ===============================
app.get('/holding/users', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso' });
    const { data: myClients } = await supabase.from('user_clientes').select('cliente_id').eq('user_email', email).neq('cliente_id', 'holding');
    const clientIds = [...new Set((myClients||[]).map(x => x.cliente_id))];
    if (!clientIds.length) return res.json([]);
    const { data: users } = await supabase.from('user_clientes').select('user_email,cliente_id,role').in('cliente_id', clientIds);
    const map = {};
    (users||[]).forEach(u => { if (!map[u.user_email]) map[u.user_email] = { email: u.user_email, clientes: [] }; map[u.user_email].clientes.push(u.cliente_id); });
    res.json(Object.values(map));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📋 TAREAS HOLDING
// ===============================
// Tareas holding — usan negocio_tasks como fuente de verdad (sincronizado con CRM individual)
app.get('/holding/tareas', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const negocio_id = req.query.negocio_id;
    let q = supabase.from('negocio_tasks').select('*').order('created_at', { ascending: true });
    if (negocio_id) q = q.eq('cliente_id', negocio_id);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/holding/tareas', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { negocio_id, titulo, columna, area, prioridad, responsable, fecha_limite, descripcion, recursos } = req.body;
    const { data, error } = await supabase.from('negocio_tasks').insert({
      cliente_id: negocio_id, titulo, columna: columna || 'por_hacer',
      area, prioridad, responsable, fecha_limite, descripcion, recursos,
    }).select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/holding/tareas/:id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const allowed = ['titulo','descripcion','columna','area','prioridad','responsable','fecha_limite','recursos'];
    const updates = {};
    allowed.forEach(k => { if (req.body[k] !== undefined) updates[k] = req.body[k]; });
    const { data, error } = await supabase.from('negocio_tasks').update(updates).eq('id', req.params.id).select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/holding/tareas/:id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { error } = await supabase.from('negocio_tasks').delete().eq('id', req.params.id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Resumen de tareas por usuario — para perfil en holding dashboard
app.get('/holding/tasks-summary', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { data: tasks, error } = await supabase
      .from('negocio_tasks')
      .select('cliente_id, responsable, columna, titulo');
    if (error) return res.status(500).json({ error: error.message });
    const summary = {};
    for (const t of (tasks || [])) {
      let resps = [];
      try { resps = t.responsable ? JSON.parse(t.responsable) : []; } catch {}
      const activa = t.columna !== 'terminado';
      for (const r of resps) {
        if (!summary[r]) summary[r] = { email: r, total: 0, activas: 0, por_negocio: {} };
        summary[r].total++;
        if (activa) summary[r].activas++;
        if (!summary[r].por_negocio[t.cliente_id]) summary[r].por_negocio[t.cliente_id] = { total: 0, activas: 0 };
        summary[r].por_negocio[t.cliente_id].total++;
        if (activa) summary[r].por_negocio[t.cliente_id].activas++;
      }
    }
    res.json(Object.values(summary));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 💸 HOLDING GASTOS
// ===============================

// GET /holding/gastos?mes=YYYY-MM  — list gastos for a given month (or all if no mes)
app.get('/holding/gastos', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    let q = supabase.from('holding_gastos').select('*').order('created_at', { ascending: false });
    if (req.query.mes) q = q.eq('mes', req.query.mes);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /holding/gastos  — add a new gasto
app.post('/holding/gastos', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { concepto, monto, mes, responsable } = req.body;
    if (!concepto || !monto || !mes) return res.status(400).json({ error: 'concepto, monto y mes son requeridos' });
    const { data, error } = await supabase.from('holding_gastos')
      .insert({ concepto: concepto.trim(), monto: parseFloat(monto), mes, responsable: responsable || 'Mau' })
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// DELETE /holding/gastos/:id
app.delete('/holding/gastos/:id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { error } = await supabase.from('holding_gastos').delete().eq('id', req.params.id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /holding/mtd-chart?cliente_ids=X,Y&year=2026&month=05
// Returns daily cumulative Cash Collected for current month + previous month (for MTD chart)
app.get('/holding/mtd-chart', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const ids = (req.query.cliente_ids || '').split(',').filter(Boolean);
    if (!ids.length) return res.json({ current: { cumulative: [], total: 0 }, previous: { cumulative: [], total: 0 } });

    const now = new Date();
    const y   = parseInt(req.query.year)  || now.getFullYear();
    const m   = req.query.month ? String(req.query.month).padStart(2, '0') : String(now.getMonth() + 1).padStart(2, '0');
    const mInt = parseInt(m);
    const daysInMonth = new Date(y, mInt, 0).getDate();

    const prevM_int      = mInt === 1 ? 12 : mInt - 1;
    const prevY          = mInt === 1 ? y - 1 : y;
    const prevM          = String(prevM_int).padStart(2, '0');
    const daysInPrevMonth = new Date(prevY, prevM_int, 0).getDate();

    const curFrom  = `${y}-${m}-01T00:00:00.000Z`;
    const curTo    = `${y}-${m}-${String(daysInMonth).padStart(2, '0')}T23:59:59.999Z`;
    const prevFrom = `${prevY}-${prevM}-01T00:00:00.000Z`;
    const prevTo   = `${prevY}-${prevM}-${String(daysInPrevMonth).padStart(2, '0')}T23:59:59.999Z`;

    const results = await Promise.all(ids.map(cid => Promise.all([
      supabase.from('clientes').select('cash_collected,created_at').eq('cliente_id', cid).gte('created_at', curFrom).lte('created_at', curTo),
      supabase.from('clientes').select('cash_collected,created_at').eq('cliente_id', cid).gte('created_at', prevFrom).lte('created_at', prevTo),
    ])));

    const allCur  = results.flatMap(([c])    => c.data || []);
    const allPrev = results.flatMap(([, p]) => p.data || []);

    const buildCumulative = (rows, days) => {
      const daily = Array(days).fill(0);
      rows.forEach(r => {
        const d = parseInt((r.created_at || '').slice(8, 10)) - 1;
        if (d >= 0 && d < days) daily[d] += parseFloat(r.cash_collected) || 0;
      });
      let acc = 0;
      return daily.map(v => { acc += v; return acc; });
    };

    const MONTH_LABELS = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
    const curCumulative  = buildCumulative(allCur,  daysInMonth);
    const prevCumulative = buildCumulative(allPrev, daysInPrevMonth);
    res.json({
      current:  {
        year: y, month: m, monthName: MONTH_LABELS[mInt - 1],
        days: daysInMonth,
        cumulative: curCumulative,
        total: curCumulative[curCumulative.length - 1] || 0,
      },
      previous: {
        year: prevY, month: prevM, monthName: MONTH_LABELS[prevM_int - 1],
        days: daysInPrevMonth,
        cumulative: prevCumulative,
        total: prevCumulative[prevCumulative.length - 1] || 0,
      },
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 🎨 FORMATOS
// ===============================
app.get('/formatos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('formatos').select('id,data,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id, created_at: r.created_at })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/formatos', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase.from('formatos').insert({ cliente_id: req.cliente_id, data: req.body }).select('id,created_at').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...req.body, id: data.id, created_at: data.created_at });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/formatos/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('formatos').update({ data: req.body }).eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/formatos/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('formatos').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 🔬 LABORATORIO DE CONTENIDO
// ===============================
app.get('/laboratorio', validateAccess, async (req, res) => {
  try {
    const tipo = req.query.tipo;
    let q = supabase.from('laboratorio').select('id,data,tipo,created_at').eq('cliente_id', req.cliente_id).order('created_at', { ascending: false });
    if (tipo) q = q.eq('tipo', tipo);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(r => ({ ...r.data, id: r.id, tipo: r.tipo, created_at: r.created_at })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/laboratorio', validateAccess, async (req, res) => {
  try {
    const { tipo, ...rest } = req.body;
    const { data, error } = await supabase.from('laboratorio').insert({ cliente_id: req.cliente_id, tipo: tipo || 'reel', data: rest }).select('id,created_at').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ...rest, id: data.id, tipo: tipo || 'reel', created_at: data.created_at });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/laboratorio/:id', validateAccess, async (req, res) => {
  try {
    const { tipo, ...rest } = req.body;
    const updateObj = { data: rest };
    if (tipo) updateObj.tipo = tipo;
    const { error } = await supabase.from('laboratorio').update(updateObj).eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/laboratorio/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('laboratorio').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📋 FORM TEMPLATES & RESPONSES
// ===============================

const DEFAULT_QUESTIONS = {
  onboarding: [
    { id:'q1', tipo:'text',     titulo:'¿Cómo te llamás?',                                     placeholder:'ej. Juan García',           required:true },
    { id:'q2', tipo:'text',     titulo:'¿De dónde sos?',                                       placeholder:'ej. Buenos Aires, Argentina', required:false },
    { id:'q3', tipo:'radio',    titulo:'¿Con cuál cargo te sentís más identificado?',          opciones:['Emprendedor','Empresario','CEO / Director','Freelancer','Profesional independiente','Otro'] },
    { id:'q4', tipo:'radio',    titulo:'¿Cómo me conociste?',                                  opciones:['Instagram','TikTok','YouTube','Recomendación','Google','Otro'] },
    { id:'q5', tipo:'radio',    titulo:'¿Qué formato de mi contenido te aporta más?',          opciones:['Reels','Historias','YouTube','Carruseles','Podcast'] },
    { id:'q6', tipo:'textarea', titulo:'¿Por qué elegiste ese formato?',                       placeholder:'Contame qué te engancha de ese formato…', maxlength:400 },
    { id:'q7', tipo:'radio',    titulo:'¿Qué tipo de reels te gustan más?',                    opciones:['Reels crudos/simples','Reels editados/producidos','Reels de valor/técnicos','Reels de vida personal','Todos por igual'] },
    { id:'q8', tipo:'textarea', titulo:'¿Qué hace que mis reels te gusten?',                   placeholder:'Qué tiene de especial…',                maxlength:300 },
    { id:'q9', tipo:'radio',    titulo:'¿Qué tipo de videos de YouTube te aportan más?',       opciones:['Formato hablando a cámara','Vlogs/día a día','Entrevistas','Tutoriales','No consumo YouTube'] },
    { id:'q10',tipo:'textarea', titulo:'¿Qué te hizo decir "Sí" y entrar al entrenamiento?',   placeholder:'El momento en que decidiste…',          maxlength:400 },
    { id:'q11',tipo:'textarea', titulo:'¿Qué te motivó específicamente a tomar acción?',       placeholder:'El detonante final fue…',               maxlength:400 },
    { id:'q12',tipo:'radio',    titulo:'¿Cuánto tiempo tardaste en decidir trabajar conmigo?', opciones:['Menos de 1 semana','Entre 1 semana y 1 mes','Entre 1 y 3 meses','Más de 3 meses'] },
    { id:'q13',tipo:'scale',    titulo:'Antes de la llamada de admisión, ¿qué tan convencido estabas de entrar?', min:1, max:10 },
    { id:'q14',tipo:'checkbox', titulo:'¿Cuáles eran tus principales problemas cuando buscaste mi ayuda?', opciones:['Me sentía vacío a pesar de mi éxito','No tenía propósito','Me sentía solo','Problemas con mi pareja','Me sentía un mal hombre','Otro'] },
    { id:'q15',tipo:'textarea', titulo:'¿Qué fue lo último que te convenció para tomar acción?', placeholder:'El factor decisivo fue…',             maxlength:400 },
  ],
  reporte_semanal: [
    { id:'q1', tipo:'radio',    titulo:'¿Cómo calificarías tu semana en general?',              opciones:['Muy bien 🔥','Bien ✅','Regular 😐','Difícil ⚠️'] },
    { id:'q2', tipo:'textarea', titulo:'¿Cómo está tu negocio esta semana?',                    subtitulo:'Un párrafo corto es suficiente.', placeholder:'Esta semana lancé mi nueva propuesta…', maxlength:500 },
    { id:'q3', tipo:'textarea', titulo:'¿Qué objetivos te planteaste esta semana?',             placeholder:'1. Cerrar al menos 2 ventas…',          maxlength:400 },
    { id:'q4', tipo:'textarea', titulo:'¿Cuáles lograste? ¿Qué quedó pendiente?',              placeholder:'Cerré 1 venta. El landing quedó al 80%…', maxlength:400 },
    { id:'q5', tipo:'textarea', titulo:'¿Qué problemas o desafíos encontraste esta semana?',   placeholder:'Me costó hacer seguimiento a los leads…', maxlength:500 },
    { id:'q6', tipo:'checkbox', titulo:'¿En qué áreas necesitás ayuda específica?',            opciones:['Ventas y cierre','Marketing y adquisición','Operaciones y procesos','Finanzas y pricing','Equipo y delegación','Producto o servicio','Foco y mentalidad'] },
    { id:'q7', tipo:'radio',    titulo:'¿Implementaste las recomendaciones de la sesión anterior?', opciones:['Sí, todas ✅','Algunas 🔸','Ninguna ❌','Es mi primera vez 🌱'] },
    { id:'q8', tipo:'textarea', titulo:'¿Querés agregar algo más?',                            subtitulo:'Opcional.',                               placeholder:'Cualquier cosa que quieras que tengamos en cuenta…', maxlength:400 },
  ],
};

const DEFAULT_COMPLETION = { titulo: '¡Formulario completado!', texto: 'Gracias por tomarte el tiempo. Tu consultor podrá prepararse mejor para ayudarte a partir de tus respuestas.' };

app.get('/form-template', async (req, res) => {
  try {
    const { cliente_id, tipo } = req.query;
    if (!cliente_id || !tipo) return res.status(400).json({ error: 'Faltan parámetros' });
    const { data } = await supabase.from('form_templates').select('questions').eq('cliente_id', cliente_id).eq('tipo', tipo).maybeSingle();
    const saved = data?.questions;
    const all = (Array.isArray(saved) && saved.length > 0) ? saved : (DEFAULT_QUESTIONS[tipo] || []);
    // Separate _completion entry from real questions
    const completion = all.find(q => q.id === '_completion') || DEFAULT_COMPLETION;
    const questions  = all.filter(q => q.id !== '_completion');
    res.json({ questions, completion_message: { titulo: completion.titulo || DEFAULT_COMPLETION.titulo, texto: completion.texto || DEFAULT_COMPLETION.texto } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/form-template', validateAccess, async (req, res) => {
  try {
    const { tipo, questions, completion_message } = req.body;
    if (!tipo || !Array.isArray(questions)) return res.status(400).json({ error: 'Datos inválidos' });
    // Store completion_message as a reserved _completion entry at end of array
    const cm = completion_message && (completion_message.titulo || completion_message.texto)
      ? { id: '_completion', titulo: completion_message.titulo || '', texto: completion_message.texto || '' }
      : null;
    const toSave = cm ? [...questions, cm] : questions;
    const { data, error } = await supabase.from('form_templates')
      .upsert({ cliente_id: req.cliente_id, tipo, questions: toSave, updated_at: new Date().toISOString() }, { onConflict: 'cliente_id,tipo' })
      .select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/form-response', async (req, res) => {
  try {
    const { cliente_id, tipo, alumno_id, alumno_nombre, alumno_instagram, responses } = req.body;
    if (!cliente_id || !tipo) return res.status(400).json({ error: 'Faltan campos obligatorios' });
    const row = { cliente_id, tipo, alumno_nombre: alumno_nombre||'', alumno_instagram: alumno_instagram||'', responses: responses||{} };
    if (alumno_id) row.alumno_id = alumno_id;
    const { data, error } = await supabase.from('form_responses').insert(row)
      .select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    // Save onboarding responses directly to the alumno record
    if (tipo === 'onboarding' && alumno_id) {
      await supabase.from('alumnos')
        .update({ onboarding_responses: responses || {}, onboarding_completed_at: new Date().toISOString() })
        .eq('id', alumno_id);
    }
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/form-responses', validateAccess, async (req, res) => {
  try {
    const { tipo } = req.query;
    let q = supabase.from('form_responses').select('*').eq('cliente_id', req.cliente_id).order('submitted_at', { ascending: false });
    if (tipo) q = q.eq('tipo', tipo);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/form-response/:id', validateAccess, async (req, res) => {
  try {
    const { id } = req.params;
    const { error } = await supabase.from('form_responses').delete().eq('id', id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 📊 FORM ANALYTICS
// ===============================
app.get('/form-analytics/onboarding', validateAccess, async (req, res) => {
  try {
    const cliente_id = req.cliente_id;

    const [{ data: responses }, { data: tpl }, { data: closedLeads }] = await Promise.all([
      supabase.from('form_responses').select('*').eq('cliente_id', cliente_id).eq('tipo', 'onboarding').order('submitted_at', { ascending: false }),
      supabase.from('form_templates').select('questions').eq('cliente_id', cliente_id).eq('tipo', 'onboarding').maybeSingle(),
      supabase.from('leads').select('etiquetas, etiqueta').eq('cliente_id', cliente_id).in('estado', ['Cerrado', 'Cerrada', 'Seña']),
    ]);

    if (!responses || responses.length === 0) {
      return res.json({ total: 0, questions: [], structured: {}, content_attribution: [], ai_insights: null, generated_at: new Date().toISOString() });
    }

    const savedQ = tpl?.questions;
    const allQ = (Array.isArray(savedQ) && savedQ.length > 0) ? savedQ : DEFAULT_QUESTIONS.onboarding;
    const questions = allQ.filter(q => q.id !== '_completion');

    // Structured stats for radio/checkbox/scale
    const structured = {};
    for (const q of questions) {
      if (q.tipo === 'radio' || q.tipo === 'checkbox') {
        const counts = {};
        for (const r of responses) {
          const val = r.responses?.[q.id];
          if (!val) continue;
          const vals = Array.isArray(val) ? val : [val];
          for (const v of vals) {
            if (v && String(v).trim()) counts[String(v).trim()] = (counts[String(v).trim()] || 0) + 1;
          }
        }
        if (Object.keys(counts).length) structured[q.id] = { titulo: q.titulo, tipo: q.tipo, counts };
      } else if (q.tipo === 'scale') {
        const vals = responses.map(r => Number(r.responses?.[q.id])).filter(v => v > 0);
        if (vals.length) {
          const avg = Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10;
          const distribution = vals.reduce((acc, v) => { acc[v] = (acc[v] || 0) + 1; return acc; }, {});
          structured[q.id] = { titulo: q.titulo, tipo: 'scale', avg, distribution, count: vals.length, min: q.min || 1, max: q.max || 10 };
        }
      }
    }

    // Content attribution from closed leads
    const contentCounts = {};
    for (const lead of (closedLeads || [])) {
      const tags = Array.isArray(lead.etiquetas) ? lead.etiquetas : (lead.etiqueta ? [lead.etiqueta] : []);
      for (const tag of tags) {
        if (tag && String(tag).trim()) {
          const t = String(tag).trim();
          contentCounts[t] = (contentCounts[t] || 0) + 1;
        }
      }
    }
    const content_attribution = Object.entries(contentCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([label, ventas]) => ({ label, ventas }));

    // AI insights
    let ai_insights = null;
    if (_anthropic) {
      const textQs = questions.filter(q => q.tipo === 'textarea' || q.tipo === 'text');
      const textSamples = responses.slice(0, 60).map(r => {
        const texts = {};
        for (const q of textQs) {
          const val = r.responses?.[q.id];
          if (val && String(val).trim()) texts[q.titulo] = String(val).trim();
        }
        return texts;
      }).filter(t => Object.keys(t).length > 0);

      const promptData = {
        total_respuestas: responses.length,
        preguntas: questions.map(q => ({ id: q.id, titulo: q.titulo, tipo: q.tipo })),
        estadisticas_estructuradas: structured,
        atribucion_contenido: content_attribution,
        respuestas_abiertas: textSamples,
      };

      try {
        const aiResp = await _anthropic.messages.create({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 2048,
          messages: [{
            role: 'user',
            content: `Sos un experto en marketing y ventas de alto ticket. Analizá los datos del formulario de onboarding y devolvé insights accionables.

Datos:
${JSON.stringify(promptData, null, 2)}

Devolvé ÚNICAMENTE un JSON válido (sin texto adicional) con esta estructura exacta:
{
  "resumen_ejecutivo": "2-3 oraciones sobre el perfil general y hallazgos principales",
  "perfiles_avatar": [
    { "nombre": "Nombre del perfil", "descripcion": "Descripción del avatar ideal", "porcentaje_estimado": 40, "señales": ["señal 1", "señal 2"] }
  ],
  "insights_ventas": [
    { "titulo": "Título corto", "descripcion": "Insight accionable para el equipo de ventas", "icono": "💡" }
  ],
  "insights_marketing": [
    { "titulo": "Título corto", "descripcion": "Insight accionable para marketing y contenido", "icono": "📈" }
  ],
  "objeciones_frecuentes": ["objeción 1", "objeción 2", "objeción 3"],
  "recomendaciones": ["recomendación accionable 1", "recomendación accionable 2", "recomendación accionable 3"]
}`,
          }],
        });
        const text = aiResp.content[0].text.trim();
        const jsonMatch = text.match(/\{[\s\S]*\}/);
        if (jsonMatch) ai_insights = JSON.parse(jsonMatch[0]);
      } catch (e) {
        console.error('Analytics AI error:', e.message);
      }
    }

    res.json({
      total: responses.length,
      questions: questions.map(q => ({ id: q.id, titulo: q.titulo, tipo: q.tipo })),
      structured,
      content_attribution,
      ai_insights,
      generated_at: new Date().toISOString(),
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===============================
// 🤖 IA — CONFIG
// ===============================
app.get('/ai/config', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase
      .from('ai_config')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .limit(1)
      .maybeSingle();
    res.json(data || { system_prompt: '', custom_context: '' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/ai/config', validateAccess, async (req, res) => {
  try {
    const { system_prompt, custom_context } = req.body;
    const payload = { system_prompt: system_prompt || '', custom_context: custom_context || '' };

    const { data: existing } = await supabase
      .from('ai_config')
      .select('id')
      .eq('cliente_id', req.cliente_id)
      .limit(1)
      .maybeSingle();

    let result;
    if (existing?.id) {
      result = await supabase
        .from('ai_config')
        .update(payload)
        .eq('id', existing.id)
        .select('*').single();
    } else {
      result = await supabase
        .from('ai_config')
        .insert({ cliente_id: req.cliente_id, ...payload })
        .select('*').single();
    }

    if (result.error) return res.status(500).json({ error: result.error.message });
    res.json(result.data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🤖 IA — ANALIZAR TRANSCRIPT
// ===============================
app.post('/ai/analyze', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada en el servidor' });

    const { transcript, call_id } = req.body;
    if (!transcript || transcript.trim().length < 30) {
      return res.status(400).json({ error: 'El transcript está vacío o es muy corto' });
    }

    const { data: aiConf } = await supabase
      .from('ai_config')
      .select('system_prompt, custom_context')
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();

    let systemPrompt = AI_BASE_SYSTEM;
    if (aiConf?.custom_context?.trim()) {
      systemPrompt += `\n\n## CONTEXTO DEL NEGOCIO\n${aiConf.custom_context.trim()}`;
    }
    if (aiConf?.system_prompt?.trim()) {
      systemPrompt += `\n\n## INSTRUCCIONES ADICIONALES\n${aiConf.system_prompt.trim()}`;
    }

    const userMessage = `Por favor analizá el siguiente transcript de llamada de ventas:\n\n---\n${transcript.trim()}\n---`;

    const completion = await _anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 8192,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }]
    });

    const rawResponse    = completion.content[0].text;
    const scorecard      = _parseCallScorecard(rawResponse);
    const cleanResponse  = _stripScorecardBlock(rawResponse);

    const messages = [
      { role: 'user',      content: userMessage  },
      { role: 'assistant', content: cleanResponse },
    ];

    const insertData = { cliente_id: req.cliente_id, transcript: transcript.trim(), messages };
    if (call_id)  insertData.call_id   = call_id;
    if (scorecard) insertData.scorecard = scorecard;

    const { data: saved, error: saveErr } = await supabase
      .from('call_analyses')
      .insert(insertData)
      .select('id, created_at, scorecard')
      .single();

    if (saveErr) {
      console.error('❌ AI SAVE:', saveErr);
      return res.json({ id: null, response: cleanResponse, messages, scorecard });
    }

    res.json({ id: saved.id, response: cleanResponse, messages, scorecard: saved.scorecard, created_at: saved.created_at });
  } catch (err) {
    console.error('❌ AI ANALYZE:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🤖 IA — CHAT FOLLOW-UP
// ===============================
app.post('/ai/chat', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada en el servidor' });

    const { analysis_id, message, messages: clientMessages } = req.body;
    if (!message?.trim()) return res.status(400).json({ error: 'Mensaje vacío' });

    let conversationHistory = clientMessages || [];

    if (analysis_id) {
      const { data: analysis } = await supabase
        .from('call_analyses')
        .select('messages')
        .eq('id', analysis_id)
        .eq('cliente_id', req.cliente_id)
        .maybeSingle();
      if (analysis?.messages) conversationHistory = analysis.messages;
    }

    const { data: aiConf } = await supabase
      .from('ai_config')
      .select('system_prompt, custom_context')
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();

    let systemPrompt = AI_BASE_SYSTEM;
    if (aiConf?.custom_context?.trim()) {
      systemPrompt += `\n\n## CONTEXTO DEL NEGOCIO\n${aiConf.custom_context.trim()}`;
    }
    if (aiConf?.system_prompt?.trim()) {
      systemPrompt += `\n\n## INSTRUCCIONES ADICIONALES\n${aiConf.system_prompt.trim()}`;
    }

    const updatedMessages = [...conversationHistory, { role: 'user', content: message.trim() }];

    const completion = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      system: systemPrompt,
      messages: updatedMessages
    });

    const assistantResponse = completion.content[0].text;
    const finalMessages = [...updatedMessages, { role: 'assistant', content: assistantResponse }];

    if (analysis_id) {
      await supabase
        .from('call_analyses')
        .update({ messages: finalMessages, updated_at: new Date().toISOString() })
        .eq('id', analysis_id)
        .eq('cliente_id', req.cliente_id);
    }

    res.json({ response: assistantResponse, messages: finalMessages });
  } catch (err) {
    console.error('❌ AI CHAT:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🤖 IA — LISTAR ANÁLISIS
// ===============================
app.get('/ai/analyses', validateAccess, async (req, res) => {
  try {
    const { call_id } = req.query;
    let q = supabase
      .from('call_analyses')
      .select('id, call_id, created_at, updated_at, transcript, scorecard')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false })
      .limit(50);
    if (call_id) q = q.eq('call_id', call_id);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🤖 IA — OBTENER ANÁLISIS
// ===============================
app.get('/ai/analyses/:id', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('call_analyses')
      .select('*')
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'Análisis no encontrado' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🤖 IA — ELIMINAR ANÁLISIS
// ===============================
app.delete('/ai/analyses/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase
      .from('call_analyses')
      .delete()
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — CONFIG (GET)
// ===============================
app.get('/ai/chat-config', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase
      .from('ai_config')
      .select('chat_system_prompt, chat_custom_context')
      .eq('cliente_id', req.cliente_id)
      .limit(1)
      .maybeSingle();
    res.json(data || { chat_system_prompt: '', chat_custom_context: '' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — CONFIG (SAVE)
// ===============================
app.patch('/ai/chat-config', validateAccess, async (req, res) => {
  try {
    const { chat_system_prompt, chat_custom_context } = req.body;
    const payload = {
      chat_system_prompt: chat_system_prompt || '',
      chat_custom_context: chat_custom_context || ''
    };

    const { data: existing } = await supabase
      .from('ai_config')
      .select('id')
      .eq('cliente_id', req.cliente_id)
      .limit(1)
      .maybeSingle();

    let result;
    if (existing?.id) {
      result = await supabase
        .from('ai_config')
        .update(payload)
        .eq('id', existing.id)
        .select('*').single();
    } else {
      result = await supabase
        .from('ai_config')
        .insert({ cliente_id: req.cliente_id, ...payload })
        .select('*').single();
    }

    if (result.error) return res.status(500).json({ error: result.error.message });
    res.json(result.data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — ANALIZAR CHAT
// ===============================
app.post('/ai/chat-analyze', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada en el servidor' });

    const { chat_text, images, lead_id, lead_name } = req.body;

    if ((!chat_text || chat_text.trim().length < 5) && (!images || images.length === 0)) {
      return res.status(400).json({ error: 'Pegá una conversación o adjuntá al menos un screenshot' });
    }

    const { data: aiConf } = await supabase
      .from('ai_config')
      .select('chat_system_prompt, chat_custom_context')
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();

    let systemPrompt = CHAT_BASE_SYSTEM;
    if (aiConf?.chat_custom_context?.trim()) {
      systemPrompt += `\n\n## CONTEXTO DEL NEGOCIO\n${aiConf.chat_custom_context.trim()}`;
    }
    if (aiConf?.chat_system_prompt?.trim()) {
      systemPrompt += `\n\n## INSTRUCCIONES ADICIONALES\n${aiConf.chat_system_prompt.trim()}`;
    }

    const contentBlocks = [];
    if (images && images.length > 0) {
      for (const img of images) {
        contentBlocks.push({
          type: 'image',
          source: { type: 'base64', media_type: img.mediaType || 'image/jpeg', data: img.base64 }
        });
      }
    }
    const textContent = chat_text?.trim()
      ? `Por favor analizá la siguiente conversación con un lead:\n\n---\n${chat_text.trim()}\n---`
      : 'Por favor analizá las imágenes de esta conversación con un lead.';
    contentBlocks.push({ type: 'text', text: textContent });

    const completion = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: contentBlocks }]
    });

    const assistantResponse = completion.content[0].text;
    const imgNote = images?.length > 0 ? `[${images.length} imagen(es) adjunta(s)]\n` : '';
    const messagesForDB = [
      { role: 'user', content: `${imgNote}${chat_text?.trim() || ''}`.trim() || textContent },
      { role: 'assistant', content: assistantResponse }
    ];

    const insertData = {
      cliente_id: req.cliente_id,
      chat_text: chat_text?.trim() || '',
      has_images: (images?.length || 0) > 0,
      messages: messagesForDB
    };
    if (lead_id) insertData.lead_id = lead_id;
    if (lead_name) insertData.lead_name = lead_name;

    const { data: saved, error: saveErr } = await supabase
      .from('chat_analyses')
      .insert(insertData)
      .select('id, created_at')
      .single();

    if (saveErr) {
      console.error('❌ CHAT AI SAVE:', saveErr);
      return res.json({ id: null, response: assistantResponse, messages: messagesForDB });
    }

    res.json({ id: saved.id, response: assistantResponse, messages: messagesForDB, created_at: saved.created_at });
  } catch (err) {
    console.error('❌ CHAT AI ANALYZE:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — FOLLOW-UP
// ===============================
app.post('/ai/chat-followup', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada' });

    const { analysis_id, message, images, messages: clientMessages } = req.body;
    if (!message?.trim() && (!images || images.length === 0)) {
      return res.status(400).json({ error: 'Enviá un mensaje o una imagen' });
    }

    let conversationHistory = clientMessages || [];

    if (analysis_id) {
      const { data: analysis } = await supabase
        .from('chat_analyses')
        .select('messages')
        .eq('id', analysis_id)
        .eq('cliente_id', req.cliente_id)
        .maybeSingle();
      if (analysis?.messages) conversationHistory = analysis.messages;
    }

    const { data: aiConf } = await supabase
      .from('ai_config')
      .select('chat_system_prompt, chat_custom_context')
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();

    let systemPrompt = CHAT_BASE_SYSTEM;
    if (aiConf?.chat_custom_context?.trim()) {
      systemPrompt += `\n\n## CONTEXTO DEL NEGOCIO\n${aiConf.chat_custom_context.trim()}`;
    }
    if (aiConf?.chat_system_prompt?.trim()) {
      systemPrompt += `\n\n## INSTRUCCIONES ADICIONALES\n${aiConf.chat_system_prompt.trim()}`;
    }

    // Build user content — text + optional images
    let userContentForAPI;
    if (images && images.length > 0) {
      const blocks = [];
      for (const img of images) {
        blocks.push({ type: 'image', source: { type: 'base64', media_type: img.mediaType || 'image/jpeg', data: img.base64 } });
      }
      const text = message?.trim() || 'Analizá esta imagen en el contexto de la conversación.';
      blocks.push({ type: 'text', text });
      userContentForAPI = blocks;
    } else {
      userContentForAPI = message.trim();
    }

    // For DB: strip base64, store as plain text
    const imgNote = images?.length > 0 ? `[${images.length} imagen(es)]\n` : '';
    const userContentForDB = `${imgNote}${message?.trim() || ''}`.trim();

    const updatedMessagesForDB = [...conversationHistory, { role: 'user', content: userContentForDB }];
    const apiMessages = [...conversationHistory, { role: 'user', content: userContentForAPI }];

    const completion = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      system: systemPrompt,
      messages: apiMessages
    });

    const assistantResponse = completion.content[0].text;
    const finalMessages = [...updatedMessagesForDB, { role: 'assistant', content: assistantResponse }];

    if (analysis_id) {
      await supabase
        .from('chat_analyses')
        .update({ messages: finalMessages, updated_at: new Date().toISOString() })
        .eq('id', analysis_id)
        .eq('cliente_id', req.cliente_id);
    }

    res.json({ response: assistantResponse, messages: finalMessages });
  } catch (err) {
    console.error('❌ CHAT FOLLOWUP:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — LISTAR ANÁLISIS
// ===============================
app.get('/ai/chat-analyses', validateAccess, async (req, res) => {
  try {
    const { lead_id } = req.query;
    let q = supabase
      .from('chat_analyses')
      .select('id, lead_id, lead_name, has_images, created_at, updated_at, chat_text')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false })
      .limit(50);
    if (lead_id) q = q.eq('lead_id', lead_id);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — OBTENER ANÁLISIS
// ===============================
app.get('/ai/chat-analyses/:id', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('chat_analyses')
      .select('*')
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'Análisis no encontrado' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 💬 CHAT IA — ELIMINAR ANÁLISIS
// ===============================
app.delete('/ai/chat-analyses/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase
      .from('chat_analyses')
      .delete()
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ========== TAREAS POR NEGOCIO ==========
async function sendTaskNotification(emails, taskTitle, clienteId, recursos, descripcion) {
  if (!process.env.GMAIL_USER || !process.env.GMAIL_PASS || !emails?.length) return;
  const transporter = require('nodemailer').createTransport({
    host: 'smtp.gmail.com', port: 465, secure: true,
    auth: { user: process.env.GMAIL_USER, pass: process.env.GMAIL_PASS },
  });
  const recursosHtml = recursos
    ? `<div style="margin-top:14px"><div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:#999;margin-bottom:6px">Recursos</div><div style="font-size:13px;color:#555;white-space:pre-wrap">${recursos.replace(/</g,'&lt;')}</div></div>`
    : '';
  const descHtml = descripcion
    ? `<p style="font-size:14px;color:#555;margin:12px 0 0;line-height:1.6">${descripcion.replace(/</g,'&lt;')}</p>`
    : '';
  for (const email of emails) {
    try {
      await transporter.sendMail({
        from: `CRM <${process.env.GMAIL_USER}>`,
        to: email,
        subject: `✅ Nueva tarea asignada: ${taskTitle}`,
        html: `<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f4f4f8;font-family:Inter,Arial,sans-serif">
  <div style="max-width:520px;margin:40px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.08)">
    <div style="background:#0a0b0f;padding:22px 32px">
      <div style="font-size:17px;font-weight:800;color:#e0b54a">✅ Nueva tarea asignada</div>
    </div>
    <div style="padding:28px 32px">
      <p style="font-size:14px;color:#555;margin:0 0 18px">Te asignaron una tarea en el CRM:</p>
      <div style="background:#f8f8fb;border:1px solid #e8e8f0;border-left:4px solid #e0b54a;border-radius:10px;padding:16px 20px">
        <div style="font-size:18px;font-weight:700;color:#111">${taskTitle.replace(/</g,'&lt;')}</div>
        ${descHtml}
        ${recursosHtml}
      </div>
    </div>
    <div style="padding:14px 32px 20px;border-top:1px solid #f0f0f0">
      <p style="font-size:11px;color:#bbb;margin:0">Negocio: ${clienteId}</p>
    </div>
  </div>
</body></html>`,
      });
    } catch(e) {
      console.error('📧 Error notif tarea:', e.message);
    }
  }
}

app.get('/tasks/users', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('user_clientes')
      .select('user_email, role')
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json((data || []).map(u => ({ email: u.user_email, role: u.role })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/tasks', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('negocio_tasks')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/tasks', validateAccess, async (req, res) => {
  try {
    const { titulo, descripcion, columna, area, prioridad, responsable, fecha_limite, recursos } = req.body;
    if (!titulo?.trim()) return res.status(400).json({ error: 'El título es obligatorio' });
    const { data, error } = await supabase
      .from('negocio_tasks')
      .insert({
        cliente_id: req.cliente_id,
        titulo: titulo.trim(),
        descripcion: descripcion?.trim() || null,
        columna: columna || 'por_hacer',
        area: area || null,
        prioridad: prioridad || null,
        responsable: responsable || null,
        fecha_limite: fecha_limite || null,
        recursos: recursos?.trim() || null,
      })
      .select()
      .single();
    if (error) return res.status(500).json({ error: error.message });
    let emails = [];
    try { emails = responsable ? JSON.parse(responsable) : []; } catch {}
    sendTaskNotification(emails, titulo.trim(), req.cliente_id, recursos?.trim(), descripcion?.trim());
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/tasks/:id', validateAccess, async (req, res) => {
  try {
    const allowed = ['titulo','descripcion','columna','area','prioridad','responsable','fecha_limite','recursos'];
    const updates = {};
    allowed.forEach(k => { if (req.body[k] !== undefined) updates[k] = req.body[k]; });
    // Detect newly added responsables to notify them
    let prevEmails = [];
    if (updates.responsable !== undefined) {
      const { data: prev } = await supabase.from('negocio_tasks').select('responsable,titulo,descripcion,recursos').eq('id', req.params.id).single();
      try { prevEmails = prev?.responsable ? JSON.parse(prev.responsable) : []; } catch {}
      let newEmails = [];
      try { newEmails = updates.responsable ? JSON.parse(updates.responsable) : []; } catch {}
      const added = newEmails.filter(e => !prevEmails.includes(e));
      if (added.length) {
        const titulo = updates.titulo || prev?.titulo || '';
        const recursos = updates.recursos !== undefined ? updates.recursos : prev?.recursos;
        const descripcion = updates.descripcion !== undefined ? updates.descripcion : prev?.descripcion;
        sendTaskNotification(added, titulo, req.cliente_id, recursos, descripcion);
      }
    }
    const { data, error } = await supabase
      .from('negocio_tasks')
      .update(updates)
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id)
      .select()
      .single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/tasks/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase
      .from('negocio_tasks')
      .delete()
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ========== OBJETIVOS MENSUALES ==========

function _objMonthRange(mes, año) {
  const start = new Date(año, mes - 1, 1);
  const end   = new Date(año, mes, 1);
  return { from: start.toISOString(), to: end.toISOString() };
}

// Campos permitidos por tabla para fórmulas custom (whitelist de seguridad)
const FORMULA_ALLOWED_FIELDS = {
  leads:    ['estado', 'calificado', 'descalificado'],
  calls:    ['estado'],
  clientes: [],
  ingresos: ['tipo', 'concepto'],
};

function _applyFormulaFilters(query, source, filters) {
  const allowed = FORMULA_ALLOWED_FIELDS[source] || [];
  for (const f of (filters || [])) {
    if (!allowed.includes(f.field)) continue; // silently skip campos no permitidos
    const vals = f.values || [f.value];
    if      (f.op === 'eq')     query = query.eq(f.field, f.value);
    else if (f.op === 'neq')    query = query.neq(f.field, f.value);
    else if (f.op === 'in')     query = query.in(f.field, vals);
    else if (f.op === 'not_in') query = query.not(f.field, 'in', `(${vals.map(v => `"${v}"`).join(',')})`);
  }
  return query;
}

async function executeCustomMetric(formula, clienteId, mes, año) {
  const src = formula?.source;
  if (!FORMULA_ALLOWED_FIELDS.hasOwnProperty(src)) return 0;

  const { from, to }  = _objMonthRange(mes, año);
  const isDateCol     = src === 'ingresos';
  const dateField     = isDateCol ? 'fecha' : 'created_at';
  const dateFrom      = isDateCol ? `${año}-${String(mes).padStart(2,'0')}-01` : from;
  const dateTo        = isDateCol ? new Date(año, mes, 0).toISOString().split('T')[0] : to;

  const buildBase = (filters) => {
    let q = supabase.from(src).select('id', { count: 'exact', head: true }).eq('cliente_id', clienteId);
    q = isDateCol ? q.gte(dateField, dateFrom).lte(dateField, dateTo) : q.gte(dateField, dateFrom).lt(dateField, dateTo);
    return _applyFormulaFilters(q, src, filters || []);
  };

  if (formula.aggregate === 'count') {
    const { count } = await buildBase(formula.filters);
    return count || 0;
  }
  if (formula.aggregate === 'percent') {
    const [{ count: num }, { count: den }] = await Promise.all([
      buildBase(formula.numerator_filters),
      buildBase(formula.denominator_filters),
    ]);
    if (!den) return 0;
    return Math.round((num / den) * 100 * 10) / 10;
  }
  return 0;
}

const METRIC_RESOLVERS = {
  agendas: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('calls').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  agendas_calificadas: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('calls').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).eq('estado', 'Calificada').gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  cierres: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('calls').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).in('estado', ['Cerrado', 'Cerrada', 'Seña']).gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  nuevos_clientes: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('clientes').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  facturacion: async (clienteId, mes, año) => {
    const fromDate = `${año}-${String(mes).padStart(2, '0')}-01`;
    const toDate   = new Date(año, mes, 0).toISOString().split('T')[0];
    const { data } = await supabase.from('ingresos').select('usd')
      .eq('cliente_id', clienteId).gte('fecha', fromDate).lte('fecha', toDate);
    return (data || []).reduce((s, r) => s + (r.usd || 0), 0);
  },
  leads_generados: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('leads').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  no_shows: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { count } = await supabase.from('calls').select('id', { count: 'exact', head: true })
      .eq('cliente_id', clienteId).in('estado', ['No asistió', 'No Show']).gte('created_at', from).lt('created_at', to);
    return count || 0;
  },
  show_rate: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { data } = await supabase.from('calls').select('estado')
      .eq('cliente_id', clienteId).gte('created_at', from).lt('created_at', to);
    const calls = data || [];
    if (!calls.length) return 0;
    const shows = calls.filter(c => !['No asistió', 'No Show', 'Cancelada', 'Re agenda', 'Pendiente'].includes(c.estado)).length;
    return Math.round((shows / calls.length) * 100 * 10) / 10;
  },
  close_rate: async (clienteId, mes, año) => {
    const { from, to } = _objMonthRange(mes, año);
    const { data } = await supabase.from('calls').select('estado')
      .eq('cliente_id', clienteId).gte('created_at', from).lt('created_at', to);
    const calls = data || [];
    // 'Cierre' y 'Cierre PIF' son los estados reales de la call; el lead queda como 'Cerrado'
    const CIERRE_ESTADOS = ['Cerrado', 'Cerrada', 'Seña', 'Cierre', 'Cierre PIF'];
    const NO_SHOW_ESTADOS = ['No asistió', 'No Show', 'Cancelada', 'Re agenda', 'Pendiente'];
    const shows   = calls.filter(c => !NO_SHOW_ESTADOS.includes(c.estado)).length;
    const cierres = calls.filter(c => CIERRE_ESTADOS.includes(c.estado)).length;
    if (!shows) return 0;
    return Math.round((cierres / shows) * 100 * 10) / 10;
  },
};

const VALID_METRIC_TYPES = [...Object.keys(METRIC_RESOLVERS), 'custom'];

const METRIC_DESCRIPTIONS = {
  agendas:             'Cantidad de llamadas agendadas (nuevas calls registradas en el mes)',
  agendas_calificadas: 'Cantidad de llamadas con estado Calificada en el mes',
  cierres:             'Cantidad de ventas cerradas (calls Cerrado, Cerrada o Seña) en el mes',
  nuevos_clientes:     'Cantidad de nuevos clientes agregados al CRM en el mes',
  facturacion:         'Suma del monto cobrado en USD (ingresos del mes)',
  leads_generados:     'Cantidad de leads nuevos generados en el mes',
  no_shows:            'Cantidad de personas que no asistieron a su llamada en el mes',
  show_rate:           'Porcentaje de shows sobre agendas: (Shows / Total agendas) × 100',
  close_rate:          'Porcentaje de cierres sobre shows: (Cierres / Shows) × 100',
};

app.post('/objectives/interpret', validateAccess, async (req, res) => {
  try {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo admins' });
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada' });
    const { descripcion } = req.body;
    if (!descripcion?.trim()) return res.status(400).json({ error: 'Descripción requerida' });

    const validKeys = Object.keys(METRIC_DESCRIPTIONS).join(', ');
    const prompt = `Sos un asistente de CRM. Respondé ÚNICAMENTE con un objeto JSON válido, sin texto adicional, sin markdown, sin explicaciones.

El usuario quiere agregar un objetivo mensual. Descripción: "${descripcion.trim()}"

CLAVES PREDEFINIDAS DISPONIBLES (usar exactamente estas cadenas de texto como valor de tipo_metrica):
${Object.entries(METRIC_DESCRIPTIONS).map(([k, v]) => `  "${k}" → ${v}`).join('\n')}

TABLAS DEL CRM (para fórmulas custom):
  leads: estado ("Agendado","Cerrado","Cerrada","Seña","Perdido","Primer contacto","Descubrimiento (Problemas-Objetivos)","Recurso de nutrición","PITCH VSL CHAT","VSL CHAT","Proponer Call","Calendly Enviado"), calificado (boolean), descalificado (boolean)
  calls: estado ("Pendiente","Calificada","Cerrado","Cerrada","No asistió","No Show","Cancelada","Re agenda","Seña")

⚠️ REGLAS CRÍTICAS — FUNNEL DE LEADS:
Los leads AVANZAN de estado. Un lead agendado que luego se cerró tiene estado="Cerrado", ya NO "Agendado".
NUNCA filtres solo por estado="Agendado" — perdés todos los que ya avanzaron.

Reglas correctas para fórmulas basadas en leads:
- "leads que llegaron a agendarse" → estado IN ["Agendado","Cerrado","Cerrada","Seña","Perdido Post Call","Seguimiento Post Call","Re agendado","No Show"]
- "leads calificados" → calificado=true SIN filtrar por estado (el flag persiste)
- "% de agendamiento" → numerator: estado IN ["Agendado","Cerrado","Cerrada","Seña","Perdido Post Call","Seguimiento Post Call","Re agendado","No Show"], denominator: todos los leads
- "% de close rate" o "cierres sobre shows" → numerator: estado IN ["Cerrado","Cerrada","Seña"], denominator: estado IN ["Cerrado","Cerrada","Seña","Perdido Post Call","Seguimiento Post Call","Re agendado","No Show"] (leads que llegaron a tener una llamada, excluyendo los que nunca asistieron sin agendar)

REGLAS GENERALES:
- Si la descripción encaja con una clave predefinida, usá ESA CLAVE EXACTA. Ejemplo: "agendas del mes" → tipo_metrica debe ser exactamente "agendas".
- Si no encaja, generá fórmula custom.

FORMATO si usás clave predefinida (tipo_metrica DEBE ser una de: ${validKeys}):
{"tipo_metrica":"agendas","titulo":"Agendas del mes","meta_sugerida":20,"descripcion_calculo":"Cantidad de calls nuevas registradas en el mes"}

FORMATO fórmula custom — conteo:
{"tipo_metrica":"custom","titulo":"Leads calificados","meta_sugerida":5,"descripcion_calculo":"Leads con calificado=true","formula":{"source":"leads","aggregate":"count","filters":[{"field":"calificado","op":"eq","value":true}]}}

FORMATO fórmula custom — porcentaje (ejemplo correcto de % agendamiento):
{"tipo_metrica":"custom","titulo":"% Agendamiento","meta_sugerida":30,"descripcion_calculo":"Leads que llegaron a agendarse / total leads del mes","formula":{"source":"leads","aggregate":"percent","numerator_filters":[{"field":"estado","op":"in","values":["Agendado","Cerrado","Cerrada","Seña"]}],"denominator_filters":[]}}

Si no es calculable: {"error":"No puedo calcular esto con los datos del CRM"}`;

    const response = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 512,
      messages: [{ role: 'user', content: prompt }],
    });

    const raw  = response.content[0].text.trim().replace(/```json?\n?/g, '').replace(/```/g, '').trim();
    const json = JSON.parse(raw);

    // Validar que tipo_metrica sea una clave conocida
    if (!json.error && json.tipo_metrica && !VALID_METRIC_TYPES.includes(json.tipo_metrica)) {
      console.warn(`[Objectives/Interpret] AI devolvió tipo_metrica inválido: "${json.tipo_metrica}". Raw: ${raw}`);
      return res.status(422).json({ error: `La IA no identificó correctamente la métrica (devolvió "${json.tipo_metrica}"). Intentá describir con más detalle cómo calcularlo.` });
    }

    res.json(json);
  } catch (err) {
    res.status(422).json({ error: 'No pude interpretar la descripción. Intentá ser más específico.' });
  }
});

app.get('/objectives', validateAccess, async (req, res) => {
  try {
    const mes = parseInt(req.query.mes) || new Date().getMonth() + 1;
    const año = parseInt(req.query.año) || new Date().getFullYear();
    const { data, error } = await supabase
      .from('monthly_objectives')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .eq('mes', mes)
      .eq('año', año)
      .order('created_at', { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/objectives/progress', validateAccess, async (req, res) => {
  try {
    const mes = parseInt(req.query.mes) || new Date().getMonth() + 1;
    const año = parseInt(req.query.año) || new Date().getFullYear();
    const { data: objectives, error } = await supabase
      .from('monthly_objectives')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .eq('mes', mes)
      .eq('año', año)
      .order('created_at', { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    if (!objectives?.length) return res.json([]);
    const results = await Promise.all(objectives.map(async (obj) => {
      let current = 0;
      if (obj.tipo_metrica === 'custom' && obj.formula) {
        current = await executeCustomMetric(obj.formula, req.cliente_id, mes, año);
      } else {
        const resolver = METRIC_RESOLVERS[obj.tipo_metrica];
        current = resolver ? await resolver(req.cliente_id, mes, año) : 0;
      }
      return {
        ...obj,
        current,
        porcentaje: obj.meta > 0 ? Math.min(100, Math.round((current / obj.meta) * 100)) : 0,
        logrado:    current >= obj.meta,
      };
    }));
    res.json(results);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/objectives', validateAccess, async (req, res) => {
  try {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo admins pueden crear objetivos' });
    const { titulo, tipo_metrica, meta, mes, año, formula } = req.body;
    if (!titulo?.trim()) return res.status(400).json({ error: 'El título es obligatorio' });
    if (!VALID_METRIC_TYPES.includes(tipo_metrica)) return res.status(400).json({ error: 'Tipo de métrica inválido' });
    if (tipo_metrica === 'custom' && !formula?.source) return res.status(400).json({ error: 'Fórmula requerida para tipo custom' });
    if (!meta || parseFloat(meta) <= 0) return res.status(400).json({ error: 'La meta debe ser mayor a 0' });
    const row = {
      cliente_id:   req.cliente_id,
      mes:          parseInt(mes)  || new Date().getMonth() + 1,
      año:          parseInt(año)  || new Date().getFullYear(),
      titulo:       titulo.trim(),
      tipo_metrica,
      meta:         parseFloat(meta),
      created_by:   req.user.user_email,
    };
    if (formula) row.formula = formula;
    const { data, error } = await supabase.from('monthly_objectives').insert(row).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/objectives/:id', validateAccess, async (req, res) => {
  try {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo admins pueden editar objetivos' });
    const { titulo, meta } = req.body;
    const updates = {};
    if (titulo !== undefined) {
      if (!titulo.trim()) return res.status(400).json({ error: 'El título no puede estar vacío' });
      updates.titulo = titulo.trim();
    }
    if (meta !== undefined) {
      if (parseFloat(meta) <= 0) return res.status(400).json({ error: 'La meta debe ser mayor a 0' });
      updates.meta = parseFloat(meta);
    }
    if (!Object.keys(updates).length) return res.status(400).json({ error: 'Nada que actualizar' });
    const { data, error } = await supabase.from('monthly_objectives').update(updates)
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/objectives/:id', validateAccess, async (req, res) => {
  try {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Solo admins pueden eliminar objetivos' });
    const { error } = await supabase.from('monthly_objectives').delete()
      .eq('id', req.params.id).eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ============================================================
// 📊 WEEKLY REPORTS — Constantes (espejo de script.js frontend)
// ============================================================

const REPORT_FUNNEL_FASES = [
  { label: 'Primer Contacto',  estados: ['Primer contacto'] },
  { label: 'Descubrimiento',   estados: ['Descubrimiento (Problemas-Objetivos)'] },
  { label: 'Nutrición',        estados: ['Recurso de nutrición'] },
  { label: 'Agendamiento',     estados: ['PITCH VSL CHAT', 'VSL CHAT', 'Proponer Call', 'Calendly Enviado'] },
  { label: 'Cierre',           estados: ['Agendado'] },
  { label: 'Cerrados',         estados: ['Cerrado', 'Cerrada', 'Seña'] },
];
const REPORT_ESTADO_CERRADO = new Set(['Cerrado', 'Cerrada', 'Seña']);
const REPORT_ESTADO_PERDIDO = new Set(['Perdido']);

const WEEKLY_REPORT_SYSTEM = `Sos un analista de negocios especializado en negocios de alto ticket en el mercado hispanohablante. Analizás métricas semanales y generás reportes ejecutivos accionables.

REGLA FUNDAMENTAL: Respondés ÚNICAMENTE con un objeto JSON válido. Sin markdown. Sin bloques de código. Sin texto antes ni después. Solo el JSON puro.

Estructura exacta requerida (todos los campos obligatorios):
{
  "resumen_ejecutivo": "texto de 2-3 oraciones concretas sobre ventas, leads y contenido de la semana",
  "que_funciono": ["elemento 1", "elemento 2"],
  "problemas_detectados": ["elemento 1", "elemento 2"],
  "recomendaciones": ["acción concreta 1", "acción concreta 2", "acción concreta 3"],
  "riesgos": ["riesgo 1"]
}

Reglas de contenido:
- resumen_ejecutivo: 2-3 oraciones. Mencioná los números más relevantes. Incluí comparativa con semana anterior si hay datos.
- que_funciono: hasta 4 bullets con evidencia concreta de los datos. Si no hay datos positivos, devolver [].
- problemas_detectados: hasta 4 bullets. Incluir si alguna métrica bajó, hay 0 ventas, el funnel muestra cuellos de botella, o la tasa de cierre es baja.
- recomendaciones: 3-5 acciones concretas para la próxima semana (qué contenido producir, qué ángulos reforzar, qué ajustar en el proceso de ventas).
- riesgos: 1-3 alertas importantes. Si no hay riesgos reales, devolver [].

Arrays vacíos se representan como [] — nunca omitir ninguna clave.
Siempre en español rioplatense. Directo. Sin palabras vacías. Priorizás lo accionable.`;

// ============================================================
// 📊 WEEKLY REPORTS — Helpers (port of frontend attribution logic)
// ============================================================

// Port of _angPiezaLabel(p) — script.js:1455
function _reportPiezaLabel(piece) {
  const SHORT = { Historia: 'H', Reel: 'Reel', Carrusel: 'C', YouTube: 'YT' };
  const t = SHORT[piece.tipo] || piece.tipo || '';
  const parts = (piece.fecha || '').split('-');
  const d = parts.length === 3
    ? `${parseInt(parts[2], 10)}/${parseInt(parts[1], 10)}`
    : piece.fecha || '—';
  return `${t} ${d}`.trim();
}

// Port of _findContentByEtiqueta(etiqueta) — script.js:2001
// year = report's year (used as primary, year-1 as fallback)
function _reportFindPieza(etiqueta, allPiezas, year) {
  if (!etiqueta) return null;
  const parts = etiqueta.trim().split(/\s+/);
  if (parts.length < 2) return null;
  const TIPO_ALIAS = {
    'H': 'Historia', 'R': 'Reel', 'C': 'Carrusel', 'YT': 'YouTube',
    'historia': 'Historia', 'reel': 'Reel', 'carrusel': 'Carrusel', 'youtube': 'YouTube',
  };
  const tipo = TIPO_ALIAS[parts[0]] || parts[0];
  const dateStr = parts[1]; // "15/5"
  const [dayStr, monthStr] = dateStr.split('/');
  const day = parseInt(dayStr, 10);
  const month = parseInt(monthStr, 10);
  if (!day || !month) return null;
  const pad = n => String(n).padStart(2, '0');
  const f1 = `${year}-${pad(month)}-${pad(day)}`;
  const f2 = `${year - 1}-${pad(month)}-${pad(day)}`;
  return allPiezas.find(x => x.tipo === tipo && x.fecha === f1) ||
         allPiezas.find(x => x.tipo === tipo && x.fecha === f2) || null;
}

// Port of _getEtiquetas(lead) — script.js:1994
function _reportGetEtiquetas(lead) {
  if (Array.isArray(lead.etiquetas) && lead.etiquetas.length) return lead.etiquetas;
  if (lead.etiqueta) return [lead.etiqueta];
  return [];
}

// Port of _esPerdidoEfectivo(l) — script.js:1794
function _reportEsPerdido(lead) {
  return REPORT_ESTADO_PERDIDO.has(lead.estado) ||
    ((lead.seguimientos || 0) >= 4 && lead.respondio_seguimiento_4 === 'NO');
}

// Port of renderAng() attribution stats — script.js:1472
// leads   = leads in the report week
// piezas  = all contenido for the client (flat, with id + data fields merged)
// ingresos = ingresos in the report week (for facturación attribution)
// year    = report's year (for etiqueta date parsing)
function _buildAtribucion(leads, piezas, ingresos, year) {
  // Fast lookup: piezaLabel → piece (mirrors _piezaMap in renderAng)
  const piezaMap = {};
  piezas.forEach(p => { piezaMap[_reportPiezaLabel(p)] = p; });

  const angStats  = {}; // { angulo: { ventas, agendas, facturacion, calificados, descalificados } }
  const angPiezas = {}; // { angulo: { piezaLabel: { ventas, agendas } } }
  const piezaStats = {}; // { pieceId: { ventas, agendas, facturacion, pieza } }

  leads.forEach(lead => {
    const ets = _reportGetEtiquetas(lead);
    if (!ets.length) return;
    const lastEt = ets[ets.length - 1];
    const pieza = piezaMap[lastEt] || _reportFindPieza(lastEt, piezas, year);
    if (!pieza || !pieza.angulo) return;

    const ang    = pieza.angulo;
    const plabel = _reportPiezaLabel(pieza);

    if (!angStats[ang])         angStats[ang]  = { ventas: 0, agendas: 0, facturacion: 0, calificados: 0, descalificados: 0 };
    if (!angPiezas[ang])        angPiezas[ang] = {};
    if (!angPiezas[ang][plabel]) angPiezas[ang][plabel] = { ventas: 0, agendas: 0 };
    if (!piezaStats[pieza.id])  piezaStats[pieza.id] = { ventas: 0, agendas: 0, facturacion: 0, leads: 0, igsClosed: new Set(), pieza };
    piezaStats[pieza.id].leads++;

    if (REPORT_ESTADO_CERRADO.has(lead.estado)) {
      angStats[ang].ventas++;
      angPiezas[ang][plabel].ventas++;
      piezaStats[pieza.id].ventas++;
      if (lead.instagram) piezaStats[pieza.id].igsClosed.add(lead.instagram.toLowerCase());
    }
    if (lead.estado === 'Agendado') {
      angStats[ang].agendas++;
      angPiezas[ang][plabel].agendas++;
      piezaStats[pieza.id].agendas++;
    }
    if (lead.calificado === true)    angStats[ang].calificados++;
    if (lead.descalificado === true) angStats[ang].descalificados++;
  });

  // Facturación: ingresos 'Venta Nueva' → match instagram to leads → attribute to their pieza/angulo
  // Mirrors the S.ing loop in renderAng() — script.js:1510
  ingresos.forEach(ing => {
    if (ing.concepto !== 'Venta Nueva' || !(Number(ing.usd) > 0) || !ing.instagram) return;
    const igLow = ing.instagram.toLowerCase();
    const lead  = leads.find(l => (l.instagram || '').toLowerCase() === igLow);
    if (!lead) return;
    const ets   = _reportGetEtiquetas(lead);
    if (!ets.length) return;
    const pieza = piezaMap[ets[ets.length - 1]] || _reportFindPieza(ets[ets.length - 1], piezas, year);
    if (!pieza || !pieza.angulo) return;
    if (!angStats[pieza.angulo]) angStats[pieza.angulo] = { ventas: 0, agendas: 0, facturacion: 0, calificados: 0, descalificados: 0 };
    angStats[pieza.angulo].facturacion += Number(ing.usd) || 0;
    if (piezaStats[pieza.id]) piezaStats[pieza.id].facturacion += Number(ing.usd) || 0;
  });

  // Build sorted arrays
  const topAngulos = Object.entries(angStats)
    .map(([angulo, st]) => ({
      angulo,
      ventas:         st.ventas,
      agendas:        st.agendas,
      facturacion:    st.facturacion,
      calificados:    st.calificados,
      descalificados: st.descalificados,
      close_rate:     st.agendas > 0 ? Math.round(st.ventas / st.agendas * 100) : 0,
      piezas: Object.entries(angPiezas[angulo] || {})
        .map(([label, s]) => ({ label, ventas: s.ventas, agendas: s.agendas }))
        .sort((a, b) => b.ventas - a.ventas),
    }))
    .sort((a, b) => b.ventas - a.ventas);

  const topPiezas = Object.values(piezaStats)
    .map(st => ({
      id:              st.pieza.id,
      tipo:            st.pieza.tipo,
      fecha:           st.pieza.fecha,
      angulo:          st.pieza.angulo || null,
      formato:         st.pieza.formato || null,
      label:           _reportPiezaLabel(st.pieza),
      agendas:         st.agendas,
      ventas:          st.ventas,
      facturacion:     st.facturacion,
      leads_generados: st.leads,
    }))
    .sort((a, b) => {
      const ta = a.ventas > 0 ? 0 : a.agendas > 0 ? 1 : 2;
      const tb = b.ventas > 0 ? 0 : b.agendas > 0 ? 1 : 2;
      if (ta !== tb) return ta - tb;
      if (ta === 0) return b.ventas - a.ventas || b.agendas - a.agendas;
      if (ta === 1) return b.agendas - a.agendas;
      return b.leads_generados - a.leads_generados;
    });

  // Summary by content type
  const porTipo = {};
  topPiezas.forEach(p => {
    if (!porTipo[p.tipo]) porTipo[p.tipo] = { piezas: 0, agendas: 0, ventas: 0, facturacion: 0 };
    porTipo[p.tipo].piezas++;
    porTipo[p.tipo].agendas     += p.agendas;
    porTipo[p.tipo].ventas      += p.ventas;
    porTipo[p.tipo].facturacion += p.facturacion;
  });

  return { topAngulos, topPiezas, porTipo };
}

// Port of _computeLostBreakdown() + renderFunnelMetricas() — script.js:1926, 2731
function _buildFunnelSnapshot(leads) {
  const activos  = leads.filter(l => !_reportEsPerdido(l));
  const perdidos = leads.filter(l =>  _reportEsPerdido(l));
  const total    = activos.length;

  const fases = REPORT_FUNNEL_FASES.map(f => {
    const inFase = activos.filter(l => f.estados.includes(l.estado));
    return {
      label:          f.label,
      count:          inFase.length,
      pct:            total > 0 ? Math.round(inFase.length / total * 100) : 0,
      calificados:    inFase.filter(l => l.calificado    === true).length,
      descalificados: inFase.filter(l => l.descalificado === true).length,
    };
  });

  // Lost breakdown: at which stage were they lost
  const lostBreakdown = {};
  REPORT_FUNNEL_FASES.forEach(f => { lostBreakdown[f.label] = 0; });
  perdidos.forEach(lead => {
    const lostAt = (lead.estado === 'Perdido' && lead.estado_anterior)
      ? lead.estado_anterior
      : ((lead.seguimientos || 0) >= 4 && lead.respondio_seguimiento_4 === 'NO' ? lead.estado : null);
    if (!lostAt) return;
    const fase = REPORT_FUNNEL_FASES.find(f => f.estados.includes(lostAt));
    if (fase) lostBreakdown[fase.label]++;
  });

  return { total, perdidos: perdidos.length, fases, lost_breakdown: lostBreakdown };
}

// Delta string helper — mirrors _delta() in script.js:476
function _reportFmtDelta(curr, prev) {
  if (prev === 0) return curr > 0 ? '+∞%' : '0%';
  const pct = (curr - prev) / prev * 100;
  return (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%';
}

// ============================================================
// 📊 POST /reports/weekly/generate
// ============================================================
app.post('/reports/weekly/generate', validateAccess, async (req, res) => {
  try {
    const { fecha_inicio, fecha_fin } = req.body;
    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ error: 'Faltan fecha_inicio y fecha_fin (YYYY-MM-DD)' });
    }

    const clienteId = req.cliente_id;

    // Parse dates as UTC to avoid timezone drift
    const [y, m, d] = fecha_inicio.split('-').map(Number);
    const fechaInicioUTC = new Date(Date.UTC(y, m - 1, d));
    const year = y;

    // Previous week: same duration, immediately before fecha_inicio
    const prevEndUTC   = new Date(fechaInicioUTC); prevEndUTC.setUTCDate(fechaInicioUTC.getUTCDate() - 1);
    const prevStartUTC = new Date(prevEndUTC);      prevStartUTC.setUTCDate(prevEndUTC.getUTCDate() - 6);

    // ISO strings for timestamp fields (leads, calls, clientes)
    const curStartTs  = `${fecha_inicio}T00:00:00.000Z`;
    const curEndTs    = `${fecha_fin}T23:59:59.999Z`;
    const prevStartTs = prevStartUTC.toISOString();
    const prevEndTs   = prevEndUTC.toISOString().replace('T00:00:00.000Z', 'T23:59:59.999Z');

    // ISO date strings for date fields (ingresos.fecha, egresos.fecha)
    const prevStartDate = prevStartUTC.toISOString().slice(0, 10);
    const prevEndDate   = prevEndUTC.toISOString().slice(0, 10);

    // All queries in parallel — leads need estado_anterior for lost breakdown
    const leadsFields = LEADS_LITE_FIELDS + ',estado_anterior';
    const [
      leadsNowRes,
      leadsPrevRes,
      ingCurRes,
      ingPrevRes,
      egresosCurRes,
      callsCurRes,
      callsPrevRes,
      cliCurRes,
      cliPrevRes,
      contenidoRes,
    ] = await Promise.all([
      supabase.from('leads').select(leadsFields)
        .eq('cliente_id', clienteId).gte('created_at', curStartTs).lte('created_at', curEndTs),
      supabase.from('leads').select(leadsFields)
        .eq('cliente_id', clienteId).gte('created_at', prevStartTs).lte('created_at', prevEndTs),
      supabase.from('ingresos').select('usd,fecha,concepto,instagram,tipo')
        .eq('cliente_id', clienteId).gte('fecha', fecha_inicio).lte('fecha', fecha_fin),
      supabase.from('ingresos').select('usd,fecha,concepto')
        .eq('cliente_id', clienteId).gte('fecha', prevStartDate).lte('fecha', prevEndDate),
      supabase.from('egresos').select('usd,fecha,tipo')
        .eq('cliente_id', clienteId).gte('fecha', fecha_inicio).lte('fecha', fecha_fin),
      supabase.from('calls').select('id,estado,created_at')
        .eq('cliente_id', clienteId).gte('created_at', curStartTs).lte('created_at', curEndTs),
      supabase.from('calls').select('id,estado')
        .eq('cliente_id', clienteId).gte('created_at', prevStartTs).lte('created_at', prevEndTs),
      supabase.from('clientes').select('id,cash_collected,created_at')
        .eq('cliente_id', clienteId).gte('created_at', curStartTs).lte('created_at', curEndTs),
      supabase.from('clientes').select('id,cash_collected')
        .eq('cliente_id', clienteId).gte('created_at', prevStartTs).lte('created_at', prevEndTs),
      supabase.from('contenido').select('id,data')
        .eq('cliente_id', clienteId),
    ]);

    // Fail fast on critical queries
    for (const [name, r] of [
      ['leads actuales', leadsNowRes], ['ingresos', ingCurRes], ['contenido', contenidoRes],
    ]) {
      if (r.error) return res.status(500).json({ error: `Query "${name}": ${r.error.message}` });
    }

    const leadsNow   = leadsNowRes.data   || [];
    const leadsPrev  = leadsPrevRes.data  || [];
    const ingCur     = ingCurRes.data     || [];
    const ingPrev    = ingPrevRes.data    || [];
    const egresosCur = egresosCurRes.data || [];
    const callsCur   = callsCurRes.data   || [];
    const callsPrev  = callsPrevRes.data  || [];
    const cliCur     = cliCurRes.data     || [];
    const cliPrev    = cliPrevRes.data    || [];

    // Flatten contenido: merge data JSONB with top-level id (mirrors GET /contenido mapping)
    const piezas = (contenidoRes.data || []).map(r => ({ id: r.id, ...(r.data || {}) }));

    // ── Métricas de ventas (semana actual) ──
    const cerradosNow    = leadsNow.filter(l => REPORT_ESTADO_CERRADO.has(l.estado));
    const agendasCount   = leadsNow.filter(l => l.estado === 'Agendado').length;
    const facturacion    = ingCur.filter(i => i.concepto === 'Venta Nueva').reduce((s, i) => s + (Number(i.usd) || 0), 0);
    const cashCollected  = cliCur.reduce((s, c) => s + (Number(c.cash_collected) || 0), 0);
    const egresoTotal    = egresosCur.reduce((s, e) => s + (Number(e.usd) || 0), 0);
    const showsCount     = callsCur.filter(c => !['No asistió', 'Cancelada', 'Re agenda', 'Pendiente'].includes(c.estado)).length;
    const aov            = cerradosNow.length > 0 ? Math.round(facturacion / cerradosNow.length) : 0;

    // ── Métricas semana anterior ──
    const cerradosPrev       = leadsPrev.filter(l => REPORT_ESTADO_CERRADO.has(l.estado));
    const facturacionPrev    = ingPrev.filter(i => i.concepto === 'Venta Nueva').reduce((s, i) => s + (Number(i.usd) || 0), 0);
    const cashCollectedPrev  = cliPrev.reduce((s, c) => s + (Number(c.cash_collected) || 0), 0);

    // ── Funnel snapshot ──
    const funnel = _buildFunnelSnapshot(leadsNow);

    // ── Atribución: ángulos + piezas ──
    const atribucion = _buildAtribucion(leadsNow, piezas, ingCur, year);

    // ── Comparativa ──
    const comparativa = {
      semana_anterior: {
        leads:          leadsPrev.length,
        cerrados:       cerradosPrev.length,
        facturacion:    facturacionPrev,
        cash_collected: cashCollectedPrev,
        calls:          callsPrev.length,
      },
      delta_leads:          _reportFmtDelta(leadsNow.length, leadsPrev.length),
      delta_cerrados:       _reportFmtDelta(cerradosNow.length,    cerradosPrev.length),
      delta_facturacion:    _reportFmtDelta(facturacion,           facturacionPrev),
      delta_cash_collected: _reportFmtDelta(cashCollected,         cashCollectedPrev),
      delta_calls:          _reportFmtDelta(callsCur.length,       callsPrev.length),
    };

    // ── Objeto metricas final ──
    const metricas = {
      ventas: {
        leads: leadsNow.length,
        agendas:           agendasCount,
        cerrados:          cerradosNow.length,
        facturacion,
        cash_collected:    cashCollected,
        egresos:           egresoTotal,
        aov,
        calls:             callsCur.length,
        shows:             showsCount,
        tasa_cierre:       agendasCount > 0 ? Math.round(cerradosNow.length / agendasCount * 100) : 0,
      },
      funnel,
      contenido: {
        total_piezas_con_datos: atribucion.topPiezas.length,
        por_tipo: atribucion.porTipo,
      },
      top_angulos: atribucion.topAngulos,
      top_piezas:  atribucion.topPiezas.slice(0, 10),
      comparativa,
    };

    // ── Persist ──
    const { data: saved, error: saveErr } = await supabase
      .from('negocio_weekly_reports')
      .insert({ cliente_id: clienteId, fecha_inicio, fecha_fin, metricas, insights_ia: null })
      .select()
      .single();

    if (saveErr) {
      console.error('❌ Save weekly report:', saveErr);
      // Return computed data even if save fails — frontend can still use it
      return res.json({ id: null, cliente_id: clienteId, fecha_inicio, fecha_fin, metricas, insights_ia: null });
    }

    console.log(`📊 Weekly report generated: ${fecha_inicio} → ${fecha_fin} (cliente: ${clienteId})`);
    res.json(saved);
  } catch (err) {
    console.error('❌ POST /reports/weekly/generate:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 🤖 POST /reports/weekly/insights  — genera análisis IA
// ============================================================
app.post('/reports/weekly/insights', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada' });

    const { report_id, metricas: metricasBody, fecha_inicio, fecha_fin } = req.body;

    // Load from DB if report_id provided, otherwise use body metricas
    let metricas = metricasBody;
    let fechaIni = fecha_inicio;
    let fechaFin = fecha_fin;

    if (report_id) {
      const { data: rep } = await supabase
        .from('negocio_weekly_reports')
        .select('metricas, fecha_inicio, fecha_fin')
        .eq('id', report_id)
        .eq('cliente_id', req.cliente_id)
        .maybeSingle();
      if (rep) { metricas = rep.metricas; fechaIni = rep.fecha_inicio; fechaFin = rep.fecha_fin; }
    }

    if (!metricas) return res.status(400).json({ error: 'Faltan métricas (pasá report_id o metricas)' });

    // Reuse ai_config business context — same pattern as /ai/analyze
    const { data: aiConf } = await supabase
      .from('ai_config').select('custom_context')
      .eq('cliente_id', req.cliente_id).maybeSingle();

    let systemPrompt = WEEKLY_REPORT_SYSTEM;
    if (aiConf?.custom_context?.trim()) {
      systemPrompt += `\n\n## CONTEXTO DEL NEGOCIO\n${aiConf.custom_context.trim()}`;
    }

    // Format metrics as readable text for Claude
    const v        = metricas.ventas     || {};
    const comp     = metricas.comparativa || {};
    const topAngs  = (metricas.top_angulos || []).slice(0, 5);
    const topPiezas = (metricas.top_piezas || []).slice(0, 5);
    const fases    = metricas.funnel?.fases || [];

    const metricsText = `MÉTRICAS DE LA SEMANA ${fechaIni ? `(${fechaIni} al ${fechaFin})` : ''}:

VENTAS:
- Leads nuevos: ${v.leads ?? 0} (${comp.delta_leads ?? '—'} vs semana anterior)
- Cerrados/Ventas: ${v.cerrados ?? 0} (${comp.delta_cerrados ?? '—'} vs semana anterior)
- Agendas: ${v.agendas ?? 0}
- Tasa de cierre: ${v.tasa_cierre ?? 0}%
- Facturación: $${v.facturacion ?? 0} USD (${comp.delta_facturacion ?? '—'} vs semana anterior)
- Cash Collected: $${v.cash_collected ?? 0} USD (${comp.delta_cash_collected ?? '—'} vs semana anterior)
- Calls realizadas: ${v.calls ?? 0} | Shows: ${v.shows ?? 0}
- AOV (ticket promedio): $${v.aov ?? 0} USD

SEMANA ANTERIOR:
- Leads: ${comp.semana_anterior?.leads ?? 0}
- Cerrados: ${comp.semana_anterior?.cerrados ?? 0}
- Facturación: $${comp.semana_anterior?.facturacion ?? 0} USD

FUNNEL (estado actual de leads de la semana):
${fases.map(f => `- ${f.label}: ${f.count} leads (${f.pct}%) — cal: ${f.calificados}, descal: ${f.descalificados}`).join('\n') || '- Sin datos de funnel'}

TOP ÁNGULOS (por ventas):
${topAngs.length
  ? topAngs.map((a, i) => `${i + 1}. "${a.angulo}" — ${a.ventas} ventas, ${a.agendas} agendas, cierre ${a.close_rate}%, $${a.facturacion} USD`).join('\n')
  : '- Sin datos de atribución esta semana.'}

TOP PIEZAS DE CONTENIDO:
${topPiezas.length
  ? topPiezas.map((p, i) => `${i + 1}. ${p.label} (${p.angulo || 'sin ángulo'}) — ${p.ventas} ventas, ${p.agendas} agendas, ${p.leads_generados ?? 0} leads`).join('\n')
  : '- Sin contenido atribuido esta semana.'}`.trim();

    const completion = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: `Analizá las métricas de esta semana y generá el reporte ejecutivo:\n\n${metricsText}` }],
    });

    const insights = completion.content[0].text;

    // Persist insights on the report record if report_id provided
    if (report_id) {
      await supabase
        .from('negocio_weekly_reports')
        .update({ insights_ia: insights })
        .eq('id', report_id)
        .eq('cliente_id', req.cliente_id);
    }

    res.json({ insights_ia: insights, report_id: report_id || null });
  } catch (err) {
    console.error('❌ POST /reports/weekly/insights:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 🗑 DELETE /reports/weekly/:id
// ============================================================
app.delete('/reports/weekly/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase
      .from('negocio_weekly_reports')
      .delete()
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 📊 GET /reports/weekly — lista de reportes guardados
// ============================================================
app.get('/reports/weekly', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('negocio_weekly_reports')
      .select('id, fecha_inicio, fecha_fin, created_at, insights_ia')
      .eq('cliente_id', req.cliente_id)
      .order('fecha_inicio', { ascending: false })
      .limit(52);
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 📊 GET /reports/weekly/:id — reporte completo
// ============================================================
app.get('/reports/weekly/:id', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('negocio_weekly_reports')
      .select('*')
      .eq('id', req.params.id)
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'Reporte no encontrado' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 💬 POST /reports/weekly/chat  — consultor estratégico IA
// ============================================================
app.post('/reports/weekly/chat', validateAccess, async (req, res) => {
  try {
    if (!_anthropic) return res.status(503).json({ error: 'ANTHROPIC_API_KEY no configurada' });

    const { report_id, message, history = [] } = req.body;
    if (!report_id || !message?.trim()) return res.status(400).json({ error: 'Faltan report_id o message' });

    const { data: rep } = await supabase
      .from('negocio_weekly_reports')
      .select('metricas, insights_ia, fecha_inicio, fecha_fin')
      .eq('id', report_id)
      .eq('cliente_id', req.cliente_id)
      .maybeSingle();
    if (!rep) return res.status(404).json({ error: 'Reporte no encontrado' });

    const { data: aiConf } = await supabase
      .from('ai_config').select('custom_context')
      .eq('cliente_id', req.cliente_id).maybeSingle();

    const v      = rep.metricas?.ventas     || {};
    const comp   = rep.metricas?.comparativa || {};
    const fases  = rep.metricas?.funnel?.fases || [];
    const topAngs   = (rep.metricas?.top_angulos || []).slice(0, 5);
    const topPiezas = (rep.metricas?.top_piezas  || []).slice(0, 5);
    const showRate  = (v.calls ?? 0) > 0 ? Math.round((v.shows ?? 0) / v.calls * 100) : 0;

    const reportCtx = `REPORTE SEMANAL (${rep.fecha_inicio} al ${rep.fecha_fin}):

VENTAS:
- Leads nuevos: ${v.leads ?? 0} (${comp.delta_leads ?? '—'} vs semana anterior)
- Cerrados/Ventas: ${v.cerrados ?? 0} (${comp.delta_cerrados ?? '—'} vs semana anterior)
- Agendas: ${v.agendas ?? 0}
- Calls: ${v.calls ?? 0} | Shows: ${v.shows ?? 0} | Show rate: ${showRate}%
- Tasa de cierre: ${v.tasa_cierre ?? 0}%
- Facturación: $${v.facturacion ?? 0} USD (${comp.delta_facturacion ?? '—'} vs semana anterior)
- Cash Collected: $${v.cash_collected ?? 0} USD
- AOV: $${v.aov ?? 0} USD

FUNNEL:
${fases.map(f => `- ${f.label}: ${f.count} leads (${f.pct}%)`).join('\n') || '- Sin datos'}

TOP ÁNGULOS:
${topAngs.length
  ? topAngs.map((a, i) => `${i + 1}. "${a.angulo}" — ${a.ventas} ventas, ${a.agendas} agendas, cierre ${a.close_rate}%`).join('\n')
  : '- Sin datos de atribución'}

TOP PIEZAS:
${topPiezas.length
  ? topPiezas.map((p, i) => `${i + 1}. ${p.label} (${p.angulo || 'sin ángulo'}) — ${p.ventas} ventas, ${p.agendas} agendas, ${p.leads_generados ?? 0} leads`).join('\n')
  : '- Sin datos de contenido'}`;

    let insightsCtx = '';
    if (rep.insights_ia) {
      try {
        let raw = typeof rep.insights_ia === 'string' ? rep.insights_ia : JSON.stringify(rep.insights_ia);
        raw = raw.replace(/^```(?:json)?\s*\n?/, '').replace(/\n?```\s*$/, '');
        const ins = JSON.parse(raw);
        insightsCtx = `\n\nINSIGHTS IA (análisis previo):
- Resumen: ${ins.resumen_ejecutivo || '—'}
- Qué funcionó: ${(ins.que_funciono || []).join('; ') || '—'}
- Problemas: ${(ins.problemas_detectados || []).join('; ') || '—'}
- Recomendaciones: ${(ins.recomendaciones || []).join('; ') || '—'}
- Riesgos: ${(ins.riesgos || []).join('; ') || '—'}`;
      } catch (e) {}
    }

    let systemPrompt = `Sos un consultor estratégico de negocios de alto ticket en el mercado hispanohablante. Tenés acceso completo al reporte semanal del negocio y respondés consultas concretas y accionables sobre contenido, ventas, funnel, show rate, cierres, ángulos, CTAs y optimización semanal.

${reportCtx}${insightsCtx}`;

    if (aiConf?.custom_context?.trim()) {
      systemPrompt += `\n\nCONTEXTO DEL NEGOCIO:\n${aiConf.custom_context.trim()}`;
    }

    systemPrompt += `\n\nRespondés en español rioplatense. Directo, sin paja. Basate siempre en los datos del reporte. Máximo 200 palabras por respuesta.`;

    const messages = [
      ...(Array.isArray(history) ? history : [])
        .filter(h => h.role === 'user' || h.role === 'assistant')
        .map(h => ({ role: h.role, content: String(h.content) })),
      { role: 'user', content: message.trim() },
    ];

    const completion = await _anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 512,
      system: systemPrompt,
      messages,
    });

    res.json({ reply: completion.content[0].text });
  } catch (err) {
    console.error('❌ POST /reports/weekly/chat:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===============================
// 🎮 DISCORD — OAuth + Bot integration
// ===============================

// Fire-and-forget Discord notifications — never crash the main flow
async function _discordNotify(event, payload) {
  if (!process.env.DISCORD_BOT_TOKEN) return;
  try {
    const { resolveTemplate, applyVars } = require('./discord.scheduler');
    const frontendUrl = (process.env.FRONTEND_URL || '').replace(/\/$/, '');

    if (event === 'report_submitted' || event === 'edit_approved' || event === 'edit_rejected') {
      const { alumno_id, cliente_id } = payload;
      if (!alumno_id) return;
      const { data: a } = await supabase.from('alumnos')
        .select('nombre, apellido, discord_channel_id').eq('id', alumno_id).maybeSingle();
      if (!a?.discord_channel_id) return;
      const nombre = [a.nombre, a.apellido].filter(Boolean).join(' ') || 'alumno';
      const link   = frontendUrl && cliente_id ? `${frontendUrl}/formulario_semanal.html?cliente_id=${cliente_id}&alumno_id=${alumno_id}` : '';
      const tpl    = await resolveTemplate(supabase, cliente_id, event);
      await _discord.sendChannelMessage(a.discord_channel_id, applyVars(tpl, { nombre, link }));

    } else if (event === 'edit_requested') {
      const adminCh = process.env.DISCORD_ADMIN_CHANNEL_ID;
      if (!adminCh) return;
      const { nombre, apellido, motivo } = payload;
      await _discord.sendChannelMessage(adminCh,
        `✏️ **Solicitud de edición** | ${[nombre, apellido].filter(Boolean).join(' ')} quiere editar su reporte.\n> ${motivo}`);
    }
  } catch (err) {
    console.error(`_discordNotify(${event}):`, err.message);
  }
}

// ── GET /discord/config ──
app.get('/discord/config', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase.from('discord_config').select('*').eq('cliente_id', req.cliente_id).maybeSingle();
    res.json(data || { cliente_id: req.cliente_id, enabled: false, guild_id: null, category_id: null, invite_link: null, admin_role_id: null, student_role_id: null, schedule_days: '1,5', schedule_utc_hour: 12, schedule_utc_min: 0 });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── PUT /discord/config ──
app.put('/discord/config', validateAccess, async (req, res) => {
  try {
    const { guild_id, category_id, invite_link, admin_role_id, student_role_id, enabled, schedule_days, schedule_utc_hour, schedule_utc_min } = req.body;
    const email = req.headers['x-user-email'];
    const { error } = await supabase.from('discord_config').upsert({
      cliente_id:        req.cliente_id,
      guild_id:          guild_id          || null,
      category_id:       category_id       || null,
      invite_link:       invite_link       || null,
      admin_role_id:     admin_role_id     || null,
      student_role_id:   student_role_id   || null,
      enabled:           Boolean(enabled),
      schedule_days:     schedule_days     || '1,5',
      schedule_utc_hour: parseInt(schedule_utc_hour) || 12,
      schedule_utc_min:  parseInt(schedule_utc_min)  || 0,
      updated_at:        new Date().toISOString(),
      updated_by:        email,
    }, { onConflict: 'cliente_id' });
    if (error) return res.status(500).json({ error: error.message });
    console.log(`[Discord config] cliente_id=${req.cliente_id} guild=${guild_id || '(none)'} enabled=${enabled} schedule=${schedule_days} ${schedule_utc_hour}:${schedule_utc_min} UTC`);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── GET /discord/templates — leer templates del cliente actual ──
app.get('/discord/templates', validateAccess, async (req, res) => {
  try {
    const { data } = await supabase.from('discord_templates').select('*').eq('cliente_id', req.cliente_id);
    const { DEFAULT_TEMPLATES, EVENT_LABELS } = require('./discord.scheduler');
    const rows = Object.keys(DEFAULT_TEMPLATES).map(event => {
      const saved = (data || []).find(r => r.event === event);
      return { event, label: EVENT_LABELS[event] || event, template: saved?.template ?? DEFAULT_TEMPLATES[event], enabled: saved?.enabled ?? true, is_custom: !!saved };
    });
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── PUT /discord/templates — guardar uno o varios templates ──
app.put('/discord/templates', validateAccess, async (req, res) => {
  try {
    const { templates } = req.body; // [{ event, template, enabled }]
    if (!Array.isArray(templates) || !templates.length) return res.status(400).json({ error: 'templates[] requerido' });
    const rows = templates.map(t => ({
      cliente_id: req.cliente_id,
      event:      t.event,
      template:   t.template ?? '',
      enabled:    t.enabled  ?? true,
      updated_at: new Date().toISOString(),
    }));
    const { error } = await supabase.from('discord_templates').upsert(rows, { onConflict: 'cliente_id,event' });
    if (error) return res.status(500).json({ error: error.message });
    console.log(`[Discord templates] cliente_id=${req.cliente_id} — ${rows.length} template(s) guardado(s)`);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Step 1: Validate identity then redirect to Discord consent screen ──
// GET /auth/discord/login?alumno_id=X&cliente_id=Y
app.get('/auth/discord/login', async (req, res) => {
  const { alumno_id, cliente_id } = req.query;

  if (!alumno_id || !cliente_id)
    return res.status(400).json({ error: 'Faltan alumno_id y cliente_id en la URL' });

  if (!process.env.DISCORD_CLIENT_ID || !process.env.DISCORD_REDIRECT_URI)
    return res.status(503).json({ error: 'Discord OAuth no configurado en el servidor' });

  // Validate alumno exists and belongs to this cliente
  const { data: alumno, error } = await supabase
    .from('alumnos')
    .select('id')
    .eq('id', alumno_id)
    .eq('cliente_id', cliente_id)
    .maybeSingle();

  if (error) return res.status(500).json({ error: 'Error al verificar identidad' });
  if (!alumno) return res.status(404).json({ error: 'Alumno no encontrado o no pertenece a este cliente' });

  const return_to = ['onboarding', 'formulario'].includes(req.query.return_to)
    ? req.query.return_to : 'formulario';
  res.redirect(_discordOAuth.getOAuthURL(alumno_id, cliente_id, return_to));
});

// ── Step 2: Discord redirects here after user consents ──
// GET /auth/discord/callback?code=X&state=Y
app.get('/auth/discord/callback', async (req, res) => {
  const frontendUrl = (process.env.FRONTEND_URL || '').replace(/\/$/, '');
  const { code, state } = req.query;

  if (!code || !state) {
    return res.redirect(`${frontendUrl}?discord=error&reason=missing_params`);
  }

  const ctx = _discordOAuth.parseState(state);
  if (!ctx?.alumno_id || !ctx?.cliente_id) {
    return res.redirect(`${frontendUrl}?discord=error&reason=invalid_state`);
  }

  const { alumno_id, cliente_id } = ctx;

  try {
    console.log(`[Discord OAuth] callback — alumno_id=${alumno_id} cliente_id=${cliente_id}`);

    // Exchange code for tokens
    const tokens = await _discordOAuth.exchangeCode(code);
    console.log(`[Discord OAuth] tokens OK — scope: ${tokens.scope}`);

    // Get Discord user profile
    const dUser = await _discordOAuth.getDiscordUser(tokens.access_token);
    console.log(`[Discord OAuth] Discord user: ${dUser.username} (${dUser.id})`);

    // Load alumno from DB — log the exact query params for diagnosis
    console.log(`[Discord OAuth] loading alumno — id=${alumno_id} cliente_id=${cliente_id}`);
    const { data: alumno, error: alumnoErr } = await supabase.from('alumnos')
      .select('id, nombre, apellido, instagram, cliente_id, discord_user_id, discord_channel_id')
      .eq('id', alumno_id).eq('cliente_id', cliente_id).maybeSingle();

    if (alumnoErr) console.error('[Discord OAuth] alumno fetch error:', alumnoErr.message);
    console.log(`[Discord OAuth] alumno found:`, alumno ? `${alumno.nombre} (cliente_id=${alumno.cliente_id})` : 'NULL');

    if (!alumno) {
      // Extra diagnosis: check if alumno exists with a different cliente_id
      const { data: anyAlumno } = await supabase.from('alumnos').select('id, cliente_id').eq('id', alumno_id).maybeSingle();
      console.error(`[Discord OAuth] alumno_not_found — exists with different cliente_id? ${anyAlumno ? JSON.stringify(anyAlumno) : 'no existe en absoluto'}`);
      return res.redirect(`${frontendUrl}/formulario_semanal.html?discord=error&reason=alumno_not_found&cliente_id=${cliente_id}&alumno_id=${alumno_id}`);
    }

    // Load per-client Discord config
    const { data: discordCfg } = await supabase
      .from('discord_config')
      .select('*')
      .eq('cliente_id', cliente_id)
      .maybeSingle();

    console.log(`[Discord OAuth] cliente_id=${cliente_id} discord_enabled=${discordCfg?.enabled} guild_id=${discordCfg?.guild_id || '(env fallback)'}`);

    if (discordCfg && discordCfg.enabled === false) {
      console.warn(`[Discord OAuth] Discord deshabilitado para cliente_id=${cliente_id}`);
      return res.redirect(`${frontendUrl}/formulario_semanal.html?discord=error&reason=discord_disabled&cliente_id=${cliente_id}&alumno_id=${alumno_id}`);
    }

    // cfg object passed to all service calls — merges DB config with env var fallback
    const guildCfg = { ...( discordCfg || {}), cliente_id };

    if (!guildCfg.guild_id && !process.env.DISCORD_GUILD_ID) {
      console.error(`[Discord OAuth] Sin guild_id configurado para cliente_id=${cliente_id}`);
      return res.redirect(`${frontendUrl}/formulario_semanal.html?discord=error&reason=no_guild_configured&cliente_id=${cliente_id}&alumno_id=${alumno_id}`);
    }

    // Add user to the correct guild
    await _discord.addGuildMember(dUser.id, tokens.access_token, guildCfg);
    console.log(`[Discord OAuth] guild join OK — guild=${guildCfg.guild_id || process.env.DISCORD_GUILD_ID}`);

    // Assign student role if configured
    if (guildCfg.student_role_id) {
      await _discord.addRoleToMember(dUser.id, guildCfg.student_role_id, guildCfg);
    }

    // Create private channel if not already done
    let channelId = alumno.discord_channel_id;
    let isNewChannel = false;

    if (!channelId) {
      // Fallback: search by user's Discord ID in permission overwrites before creating
      const existing = await _discord.findChannelByUser(dUser.id, guildCfg);
      if (existing) {
        channelId = existing.id;
        console.log(`[Discord OAuth] found existing channel by user overwrite: ${channelId}`);
      } else {
        const nombre   = (alumno.nombre || 'alumno').toLowerCase();
        const chanName = `cliente-${nombre}`;
        console.log(`[Discord OAuth] creating channel: ${chanName} — guild=${guildCfg.guild_id || process.env.DISCORD_GUILD_ID} category=${guildCfg.category_id || process.env.DISCORD_CATEGORY_ID || '(none)'}`);
        const chan = await _discord.createPrivateChannel(chanName, dUser.id, guildCfg);
        channelId    = chan.id;
        isNewChannel = true;
        console.log(`[Discord OAuth] channel created: ${channelId}`);
      }
    } else {
      console.log(`[Discord OAuth] channel already exists in DB: ${channelId}`);
    }

    // Send message — welcome for new channels, reconnect notice for existing ones
    const { resolveTemplate, applyVars } = require('./discord.scheduler');
    const nombre = alumno.nombre || 'alumno';
    const ig     = alumno.instagram || '';
    const base   = (frontendUrl || '').replace(/\/$/, '');
    let link;
    if (isNewChannel) {
      // Welcome: send onboarding link so the student fills their profile first
      link = base
        ? `${base}/onboarding.html?cliente_id=${encodeURIComponent(cliente_id)}&tipo=onboarding&alumno_id=${encodeURIComponent(alumno_id)}&nombre=${encodeURIComponent(nombre)}${ig ? '&ig=' + encodeURIComponent(ig) : ''}`
        : '';
    } else {
      link = base ? `${base}/formulario_semanal.html?cliente_id=${cliente_id}&alumno_id=${alumno_id}` : '';
    }
    const msgEvent = isNewChannel ? 'welcome' : 'reconnect';
    const tpl = await resolveTemplate(supabase, cliente_id, msgEvent);
    await _discord.sendChannelMessage(channelId, applyVars(tpl, { nombre, link }));

    // Persist Discord info — capture error explicitly
    const updatePayload = {
      discord_user_id:      dUser.id,
      discord_username:     dUser.global_name || dUser.username,
      discord_avatar:       dUser.avatar
        ? `https://cdn.discordapp.com/avatars/${dUser.id}/${dUser.avatar}.png`
        : null,
      discord_channel_id:   channelId,
      discord_connected_at: new Date().toISOString(),
    };
    console.log(`[Discord OAuth] updating alumnos — id=${alumno_id}`, updatePayload);
    const { data: updated, error: updateErr } = await supabase
      .from('alumnos')
      .update(updatePayload)
      .eq('id', alumno_id)
      .select('id, discord_user_id, discord_channel_id');

    if (updateErr) {
      console.error(`[Discord OAuth] UPDATE FAILED:`, updateErr.message, updateErr.details, updateErr.hint);
      // Still redirect — channel was created, just DB write failed
    } else {
      console.log(`[Discord OAuth] UPDATE OK:`, JSON.stringify(updated));
    }

    const destPage = ctx.return_to === 'onboarding' ? 'discord_onboarding.html' : 'formulario_semanal.html';
    return res.redirect(
      `${frontendUrl}/${destPage}?discord=connected&cliente_id=${cliente_id}&alumno_id=${alumno_id}`
    );
  } catch (err) {
    console.error('[Discord OAuth] callback error:', err.message, err.stack?.split('\n')[1]);
    const destPage = ctx?.return_to === 'onboarding' ? 'discord_onboarding.html' : 'formulario_semanal.html';
    return res.redirect(
      `${frontendUrl}/${destPage}?discord=error&reason=server_error&cliente_id=${cliente_id}&alumno_id=${alumno_id}`
    );
  }
});

// ── GET /alumnos/:id/discord — check connection status ──
app.get('/alumnos/:id/discord', async (req, res) => {
  try {
    const { data, error } = await supabase.from('alumnos')
      .select('discord_user_id, discord_username, discord_avatar, discord_channel_id, discord_connected_at')
      .eq('id', req.params.id).maybeSingle();
    if (error || !data) return res.status(404).json({ connected: false });
    res.json({ connected: !!data.discord_user_id, ...data });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── GET /discord/debug-students — show all alumnos with discord fields (no client filter) ──
app.get('/discord/debug-students', async (req, res) => {
  try {
    // No cliente_id filter — lets us see every row and its actual cliente_id
    const { data, error } = await supabase
      .from('alumnos')
      .select('id, nombre, apellido, cliente_id, discord_user_id, discord_channel_id, discord_connected_at')
      .order('discord_connected_at', { ascending: false })
      .limit(50);

    if (error) return res.status(500).json({ error: error.message, hint: 'Las columnas discord_* pueden no existir — ejecutá la migración SQL' });

    const connected   = (data || []).filter(a => a.discord_user_id);
    const withChannel = (data || []).filter(a => a.discord_channel_id);
    const ready       = (data || []).filter(a => a.discord_user_id && a.discord_channel_id);

    res.json({
      total_rows:        data?.length ?? 0,
      with_discord_user: connected.length,
      with_channel:      withChannel.length,
      ready_to_receive:  ready.length,
      rows:              data,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /discord/debug — diagnose why no alumnos are found (admin only) ──
app.get('/discord/debug', async (req, res) => {
  const cliente_id = req.headers['x-cliente-id'];
  if (!cliente_id) return res.status(400).json({ error: 'Falta x-cliente-id' });
  req.cliente_id = cliente_id;
  try {
    // Check if columns exist by fetching one row with all discord fields
    const { data: sample, error: colErr } = await supabase
      .from('alumnos')
      .select('id, nombre, discord_user_id, discord_channel_id, discord_connected_at')
      .eq('cliente_id', req.cliente_id)
      .limit(3);

    if (colErr) return res.json({ error: colErr.message, hint: 'Las columnas discord_* probablemente no existen — ejecutá la migración SQL' });

    // Count alumnos total vs connected
    const { count: total } = await supabase
      .from('alumnos')
      .select('id', { count: 'exact', head: true })
      .eq('cliente_id', req.cliente_id);

    const { count: withUserId } = await supabase
      .from('alumnos')
      .select('id', { count: 'exact', head: true })
      .eq('cliente_id', req.cliente_id)
      .not('discord_user_id', 'is', null);

    const { count: withChannel } = await supabase
      .from('alumnos')
      .select('id', { count: 'exact', head: true })
      .eq('cliente_id', req.cliente_id)
      .not('discord_channel_id', 'is', null);

    const { count: ready } = await supabase
      .from('alumnos')
      .select('id', { count: 'exact', head: true })
      .eq('cliente_id', req.cliente_id)
      .not('discord_user_id', 'is', null)
      .not('discord_channel_id', 'is', null);

    res.json({
      columns_exist: true,
      total_alumnos: total,
      with_discord_user_id: withUserId,
      with_discord_channel_id: withChannel,
      ready_to_receive: ready,
      sample_rows: sample,
      bot_token_set: !!process.env.DISCORD_BOT_TOKEN,
      frontend_url: process.env.FRONTEND_URL || '(no configurado)',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /discord/send-weekly-report — manual trigger (admin only) ──
app.post('/discord/send-weekly-report', async (req, res) => {
  if (!req.headers['x-cliente-id']) return res.status(400).json({ error: 'Falta x-cliente-id' });
  if (!process.env.DISCORD_BOT_TOKEN)
    return res.status(503).json({ error: 'DISCORD_BOT_TOKEN no configurado' });
  try {
    const stats = await _sendWeeklyReports(supabase, process.env.FRONTEND_URL);
    res.json({ ok: true, ...stats });
  } catch (err) {
    console.error('POST /discord/send-weekly-report:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════
// 🗓  CALENDLY INTEGRATION (OAuth per-negocio)
// ══════════════════════════════════════════

// In-memory webhook log (last 50 events) — kept for immediate /debug reads
const _calendlyWebhookLog = [];
function _logCalendlyWebhook(entry) {
  const record = { ...entry, at: new Date().toISOString() };
  _calendlyWebhookLog.unshift(record);
  if (_calendlyWebhookLog.length > 50) _calendlyWebhookLog.pop();
  // Also persist to DB so logs survive Railway deploys
  supabase.from('calendly_webhook_log').insert({
    event_type:    entry.eventType   || null,
    invitee_uri:   entry.inviteeUri  || null,
    cliente_id:    entry.cliente_id  || null,
    status:        entry.status      || null,
    invitee_name:  entry.name        || null,
    invitee_email: entry.email       || null,
    webhook_token: entry.webhookToken|| null,
    call_id:       entry.callId      ? String(entry.callId) : null,
    error_msg:     entry.error       || null,
  }).then(({ error }) => {
    if (error) console.warn('[Calendly Log] DB persist failed:', error.message);
  });
}

// Get and auto-refresh the token for a negocio's Calendly connection
async function _getCalendlyToken(negocio_id) {
  const { data: conn } = await supabase
    .from('calendly_connections')
    .select('*')
    .eq('negocio_id', negocio_id)
    .maybeSingle();
  if (!conn) return null;

  try {
    const result = await _calendlyOAuth.ensureFreshToken(conn);
    if (result.updated) {
      await supabase.from('calendly_connections').update({
        access_token:     result.accessToken,
        refresh_token:    result.newRefresh,
        token_expires_at: result.newExpires,
      }).eq('negocio_id', negocio_id);
      console.log(`[Calendly OAuth] Token refreshed for negocio=${negocio_id}`);
    }
    return { ...conn, access_token: result.accessToken };
  } catch (err) {
    console.error(`[Calendly OAuth] Token refresh failed negocio=${negocio_id}:`, err.message);
    return conn; // return stale token, let the caller handle the error
  }
}

// Update matching lead estado to 'Agendado' — try instagram first, fallback to nombre
async function _calendlyUpdateLeadAgendado(instagram, name, cliente_id) {
  const FINAL = ['Cerrado', 'Seña', 'Perdido', 'Perdido Post Call', 'No Show'];
  let lead = null;
  let matchedBy = '';

  if (instagram) {
    const { data } = await supabase.from('leads')
      .select('id, estado')
      .eq('cliente_id', cliente_id)
      .ilike('instagram', instagram)
      .limit(1)
      .maybeSingle();
    if (data) { lead = data; matchedBy = 'instagram'; }
  }

  if (!lead && name && name !== 'Sin nombre') {
    const { data } = await supabase.from('leads')
      .select('id, estado')
      .eq('cliente_id', cliente_id)
      .ilike('nombre', name)
      .order('updated_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (data) { lead = data; matchedBy = 'nombre'; }
  }

  if (!lead) {
    console.log(`[Calendly] No lead match for Agendado — ig="${instagram}" name="${name}"`);
    return;
  }
  if (FINAL.includes(lead.estado)) {
    console.log(`[Calendly] Lead id=${lead.id} already in final state "${lead.estado}" — skipping`);
    return;
  }

  const { error } = await supabase.from('leads')
    .update({ estado: 'Agendado', updated_at: new Date().toISOString() })
    .eq('id', lead.id)
    .eq('cliente_id', cliente_id);

  if (error) console.warn(`[Calendly] Lead Agendado update failed: ${error.message}`);
  else console.log(`[Calendly] ✓ Lead id=${lead.id} → Agendado (matched by ${matchedBy})`);
}

// Insert a new call into the `calls` table (the table "Llamadas de venta" reads from)
async function _calendlyCreateCall(inv, cliente_id, extraFields = {}) {
  // Count existing calls for this instagram to set numero_llamada
  const instagramKey = inv.instagram || '';
  let numero_llamada = 1;
  if (instagramKey) {
    const { data: prev } = await supabase.from('calls')
      .select('id').eq('instagram', instagramKey).eq('cliente_id', cliente_id);
    numero_llamada = (prev?.length || 0) + 1;
  }

  // Form responses stored as JSONB — visible in "Reporte Calendly" column; info_previa left for setter
  const formResponsesObj = Object.keys(inv.formResponses || {}).length > 0 ? inv.formResponses : null;

  // Full row with all Calendly-specific columns
  const fullRow = {
    cliente_id,
    nombre:                    inv.name      || 'Sin nombre',
    instagram:                 instagramKey,
    whatsapp:                  inv.telefono  || '',
    origen:                    'Calendly',
    estado:                    'Pendiente',
    numero_llamada,
    seguimientos:              0,
    responde:                  false,
    fecha_llamada:             inv.startTime  || null,
    link_llamada:              inv.meetingLink || null,
    calendly_invitee_uri:      inv.uri        || null,
    email:                     inv.email      || null,
    calendly_form_responses:   formResponsesObj,
    ...extraFields,
  };

  console.log(`[Calendly Lead Sync] Attempting INSERT into calls — cliente=${cliente_id} nombre="${inv.name}" email=${inv.email} fecha=${inv.startTime}`);

  let { data, error } = await supabase.from('calls').insert(fullRow).select('id').single();

  if (error) {
    // Graceful fallback: drop optional columns that may not exist yet
    console.warn(`[Calendly Error] Full insert failed (${error.message}) — retrying with core fields`);
    const coreRow = {
      cliente_id,
      nombre:        inv.name     || 'Sin nombre',
      instagram:     instagramKey,
      whatsapp:      inv.telefono || '',
      origen:        'Calendly',
      estado:        'Pendiente',
      numero_llamada,
      seguimientos:  0,
      responde:      false,
      fecha_llamada: inv.startTime   || null,
      link_llamada:  inv.meetingLink || null,
    };
    ({ data, error } = await supabase.from('calls').insert(coreRow).select('id').single());
    if (error) {
      console.error(`[Calendly Error] Core insert also failed: ${error.message}`, { fullRow: coreRow });
      throw new Error(`INSERT call failed: ${error.message}`);
    }
  }

  console.log(`[Calendly Lead Created] ✓ call.id=${data.id} cliente=${cliente_id} nombre="${inv.name}" email=${inv.email} fecha=${inv.startTime}`);

  // Auto-move matching lead to 'Agendado' in the pipeline
  await _calendlyUpdateLeadAgendado(instagramKey, inv.name, cliente_id).catch(e => {
    console.warn('[Calendly] Lead Agendado update error:', e.message);
  });

  return data.id;
}

// POST /webhooks/calendly — receives Calendly webhook events (OAuth token + legacy mapping)
app.post('/webhooks/calendly', async (req, res) => {
  try {
    const sigHeader  = req.headers['calendly-webhook-signature'];
    const signingKey = process.env.CALENDLY_WEBHOOK_SIGNING_KEY;
    const rawBody    = req.rawBody ? req.rawBody.toString() : JSON.stringify(req.body);

    console.log(`[Calendly Webhook Received] method=POST token=${req.query.t || 'none'} sig=${sigHeader ? 'present' : 'absent'}`);

    // Signature check (skipped if CALENDLY_WEBHOOK_SIGNING_KEY not set)
    if (signingKey) {
      if (!_calendlyVerify(rawBody, sigHeader, signingKey)) {
        console.warn('[Calendly Webhook] ❌ Invalid signature — rejected');
        return res.status(401).json({ error: 'Invalid signature' });
      }
      console.log('[Calendly Webhook] ✓ Signature valid');
    } else {
      console.warn('[Calendly Webhook] ⚠ No signing key configured — skipping signature check');
    }

    const { event: eventType, payload } = req.body;
    if (!eventType || !payload) {
      console.error('[Calendly Webhook] ❌ Missing event or payload in body');
      return res.status(400).json({ error: 'Missing event or payload' });
    }

    console.log(`[Calendly Webhook] event=${eventType}`);

    // Extract invitee data and log the full parsed result
    const inv = _calendlyExtract(payload);
    console.log(`[Calendly Payload] name="${inv.name}" email=${inv.email} phone=${inv.telefono || '—'} eventTypeUri=${inv.eventTypeUri || '—'} inviteeUri=${inv.uri || '—'} startTime=${inv.startTime || '—'} meetingLink=${inv.meetingLink || '—'}`);
    if (Object.keys(inv.formResponses).length) {
      console.log(`[Calendly Payload] formResponses: ${JSON.stringify(inv.formResponses)}`);
    }

    let cliente_id = null;

    // ── Primary: OAuth connection identified by URL token (?t=TOKEN) ──
    const webhookToken = req.query.t;
    if (webhookToken) {
      const { data: conn, error: connErr } = await supabase
        .from('calendly_connections')
        .select('negocio_id')
        .eq('webhook_token', webhookToken)
        .maybeSingle();
      if (connErr) console.warn('[Calendly Webhook] Token lookup error:', connErr.message);
      if (conn) {
        cliente_id = conn.negocio_id;
        console.log(`[Calendly Webhook] ✓ Negocio identified by OAuth token → cliente_id=${cliente_id}`);
      } else {
        console.warn(`[Calendly Webhook] ⚠ Token "${webhookToken}" not found in calendly_connections`);
      }
    }

    // ── Fallback: legacy event_type URI → calendly_event_mappings ──
    if (!cliente_id && inv.eventTypeUri) {
      const { data: mapping } = await supabase
        .from('calendly_event_mappings')
        .select('cliente_id')
        .eq('calendly_event_uri', inv.eventTypeUri)
        .maybeSingle();
      if (mapping) {
        cliente_id = mapping.cliente_id;
        console.log(`[Calendly Event Mapped] event_uri=${inv.eventTypeUri} → cliente_id=${cliente_id}`);
      }
    }

    if (!cliente_id) {
      console.warn(`[Calendly Webhook] ❌ Could not identify negocio — token=${webhookToken || 'none'} eventTypeUri=${inv.eventTypeUri || 'none'}`);
      _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id: null, status: 'no_mapping', name: inv.name, email: inv.email });
      return res.json({ ok: true, warning: 'No mapping found — event logged but no lead created' });
    }

    _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'processing', name: inv.name, email: inv.email });

    // ── invitee.created ──
    // Calendly also fires this for reschedules (with payload.old_invitee set).
    // When old_invitee is present → update the existing call instead of creating a duplicate.
    if (eventType === 'invitee.created') {
      const oldInviteeUri = inv.oldInviteeUri;

      if (oldInviteeUri) {
        // Reschedule: update the original call with new date/link/uri
        console.log(`[Calendly Webhook] Reschedule detected — updating old call old=${oldInviteeUri} new=${inv.uri}`);
        const { data: updated, error } = await supabase.from('calls')
          .update({
            calendly_invitee_uri: inv.uri,
            fecha_llamada:        inv.startTime  || null,
            link_llamada:         inv.meetingLink || null,
            estado:               'Pendiente',
            reagendada:           true,
          })
          .eq('cliente_id', cliente_id)
          .eq('calendly_invitee_uri', oldInviteeUri)
          .select('id');

        if (error) {
          console.warn('[Calendly Webhook] ⚠ Reschedule update error:', error.message);
          _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'error', name: inv.name, email: inv.email, error: error.message });
        } else if (!updated || updated.length === 0) {
          // Original call not in CRM (webhook may have been lost earlier) — create it now
          console.warn(`[Calendly Webhook] ⚠ Reschedule: original call not found (old=${oldInviteeUri}) — creating new call as fallback`);
          const callId = await _calendlyCreateCall(inv, cliente_id, { reagendada: true });
          _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'reschedule_fallback_created', callId, name: inv.name, email: inv.email });
        } else {
          console.log(`[Calendly Webhook] ✓ Rescheduled rows=${updated.length} nueva_fecha=${inv.startTime}`);
          _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'rescheduled', callId: updated[0]?.id, name: inv.name, email: inv.email });
        }
      } else {
        // New booking — dedup check first
        if (inv.uri) {
          const { data: dup } = await supabase.from('calls').select('id')
            .eq('cliente_id', cliente_id).eq('calendly_invitee_uri', inv.uri).maybeSingle();
          if (dup) {
            console.log(`[Calendly Webhook] ⚠ Duplicate invitee_uri=${inv.uri} — skipped (call.id=${dup.id})`);
            return res.json({ ok: true, info: 'duplicate', call_id: dup.id });
          }
        }
        const callId = await _calendlyCreateCall(inv, cliente_id);
        _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'created', callId, name: inv.name, email: inv.email });
      }

    // ── invitee.canceled ──
    // When it's a reschedule Calendly also fires canceled (with payload.new_invitee set).
    // In that case skip marking as 'No asistió' — the invitee.created handler will update the row.
    } else if (eventType === 'invitee.canceled') {
      const isReschedule = !!(payload.new_invitee);
      console.log(`[Calendly Webhook] Processing cancellation uri=${inv.uri} isReschedule=${isReschedule}`);
      if (isReschedule) {
        console.log('[Calendly Webhook] Skipping "No asistió" — this canceled event is part of a reschedule');
        _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'reschedule_cancel_skipped', name: inv.name, email: inv.email });
      } else {
        const { data: updated, error } = await supabase.from('calls')
          .update({ estado: 'Cancelada' })
          .eq('cliente_id', cliente_id)
          .eq('calendly_invitee_uri', inv.uri)
          .select('id');
        if (error) console.warn('[Calendly Webhook] ⚠ Cancel update error:', error.message);
        else console.log(`[Calendly Webhook] ✓ invitee.canceled → "Cancelada" rows=${updated?.length || 0} uri=${inv.uri}`);
        _logCalendlyWebhook({ eventType, inviteeUri: inv.uri, cliente_id, status: 'canceled', name: inv.name, email: inv.email });
      }

    } else {
      console.log(`[Calendly Webhook] Unhandled event type: ${eventType}`);
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[Calendly Error] Webhook handler failed:', err.message, err.stack?.split('\n')[1] || '');
    _logCalendlyWebhook({ eventType: req.body?.event, status: 'error', error: err.message });
    res.status(500).json({ error: err.message });
  }
});

// GET /calendly/connect?cliente_id=X — redirect to Calendly OAuth
app.get('/calendly/connect', (req, res) => {
  const { cliente_id } = req.query;
  if (!cliente_id) return res.status(400).send('Missing cliente_id');
  if (!process.env.CALENDLY_CLIENT_ID) return res.status(503).send('CALENDLY_CLIENT_ID not configured');

  const backendUrl  = process.env.BACKEND_URL || `${req.protocol}://${req.get('host')}`;
  const redirectUri = `${backendUrl}/calendly/callback`;
  const state       = Buffer.from(JSON.stringify({ cliente_id, nonce: require('crypto').randomBytes(8).toString('hex') })).toString('base64url');

  const authUrl = _calendlyOAuth.buildOAuthURL(redirectUri, state);
  console.log(`[Calendly OAuth] Redirecting negocio=${cliente_id} to Calendly auth`);
  res.redirect(authUrl);
});

// GET /calendly/callback — OAuth callback from Calendly
app.get('/calendly/callback', async (req, res) => {
  const { code, state, error: oauthError } = req.query;
  const frontendUrl = (process.env.FRONTEND_URL || '').replace(/\/$/, '');

  if (oauthError) {
    console.warn('[Calendly OAuth] User denied access:', oauthError);
    return res.send(_calendlyPopupPage(false, null, `Acceso denegado: ${oauthError}`));
  }
  if (!code || !state) return res.status(400).send('Missing code or state');

  let cliente_id;
  try {
    const decoded = JSON.parse(Buffer.from(state, 'base64url').toString());
    cliente_id    = decoded.cliente_id;
    if (!cliente_id) throw new Error('No cliente_id in state');
  } catch (err) {
    return res.status(400).send('Invalid state parameter');
  }

  try {
    const backendUrl  = process.env.BACKEND_URL || `${req.protocol}://${req.get('host')}`;
    const redirectUri = `${backendUrl}/calendly/callback`;

    // Exchange code for tokens
    console.log(`[Calendly OAuth] Exchanging code for tokens — negocio=${cliente_id}`);
    const tokens   = await _calendlyOAuth.exchangeCode(code, redirectUri);
    const expiresAt = new Date(Date.now() + (tokens.expires_in || 7200) * 1000).toISOString();

    // Get user info
    const user = await _calendlyOAuth.getCurrentUser(tokens.access_token);
    const userUri = user.uri;
    const orgUri  = user.current_organization;
    console.log(`[Calendly OAuth] Connected user=${user.email} org=${orgUri} negocio=${cliente_id}`);

    // Generate per-negocio webhook token for URL-based identification
    const webhookToken = _calendlyOAuth.generateWebhookToken();
    const webhookUrl   = `${backendUrl}/webhooks/calendly?t=${webhookToken}`;

    // Delete existing webhook subscription if any (to avoid duplicates)
    const { data: existing } = await supabase
      .from('calendly_connections').select('webhook_uri, access_token').eq('negocio_id', cliente_id).maybeSingle();
    if (existing?.webhook_uri) {
      try {
        await _calendlyOAuth.deleteWebhookSubscription(existing.access_token || tokens.access_token, existing.webhook_uri);
        console.log(`[Calendly OAuth] Deleted old webhook ${existing.webhook_uri}`);
      } catch (e) { console.warn('[Calendly OAuth] Could not delete old webhook:', e.message); }
    }

    // Create new webhook subscription
    let webhookUri = null;
    try {
      const webhook = await _calendlyOAuth.createWebhookSubscription(tokens.access_token, orgUri, userUri, webhookUrl);
      webhookUri    = webhook?.uri || null;
      console.log(`[Calendly OAuth] Webhook created: ${webhookUri}`);
    } catch (e) {
      console.warn('[Calendly OAuth] Webhook creation warning:', e.message, '— connection will still be saved');
    }

    // Upsert connection in DB
    const connRow = {
      negocio_id:          cliente_id,
      calendly_user_uri:   userUri,
      calendly_org_uri:    orgUri,
      calendly_user_name:  user.name  || null,
      calendly_user_email: user.email || null,
      access_token:        tokens.access_token,
      refresh_token:       tokens.refresh_token || null,
      token_expires_at:    expiresAt,
      webhook_uri:         webhookUri,
      webhook_token:       webhookToken,
      connected_at:        new Date().toISOString(),
    };

    const { error: upsertErr } = await supabase
      .from('calendly_connections')
      .upsert(connRow, { onConflict: 'negocio_id' });

    if (upsertErr) throw new Error(`DB upsert failed: ${upsertErr.message}`);

    // Auto-save provider preference so the holding dashboard knows this negocio uses Calendly
    await supabase.from('negocio_settings')
      .upsert({ negocio_id: cliente_id, calendar_provider: 'calendly' }, { onConflict: 'negocio_id' });

    console.log(`[Calendly OAuth] Connection saved — negocio=${cliente_id} user=${user.email}`);
    res.send(_calendlyPopupPage(true, cliente_id, null));

  } catch (err) {
    console.error('[Calendly OAuth] Callback error:', err.message);
    res.send(_calendlyPopupPage(false, cliente_id, err.message));
  }
});

// Helper: build the popup close page (sends postMessage to parent)
function _calendlyPopupPage(success, cliente_id, errorMsg) {
  const msg = success
    ? `{type:'calendly_connected', cliente_id:${JSON.stringify(cliente_id)}}`
    : `{type:'calendly_error', error:${JSON.stringify(errorMsg || 'Unknown error')}}`;
  return `<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{font-family:sans-serif;background:#0d0e12;color:#9ba0b4;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}</style>
</head><body>
<div style="text-align:center">
  <div style="font-size:32px;margin-bottom:12px">${success ? '✅' : '❌'}</div>
  <div style="font-size:14px">${success ? 'Calendly conectado. Cerrando...' : 'Error: ' + (errorMsg || '')}</div>
</div>
<script>
  try { window.opener && window.opener.postMessage(${msg}, '*'); } catch(e) {}
  setTimeout(() => window.close(), 1500);
</script>
</body></html>`;
}

// GET /calendly/connection-status — connection info for the current negocio
app.get('/calendly/connection-status', validateAccess, async (req, res) => {
  const { data, error } = await supabase
    .from('calendly_connections')
    .select('negocio_id, calendly_user_name, calendly_user_email, calendly_user_uri, webhook_uri, connected_at')
    .eq('negocio_id', req.cliente_id)
    .maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || null);
});

// DELETE /calendly/disconnect — disconnect Calendly from the current negocio
app.delete('/calendly/disconnect', validateAccess, async (req, res) => {
  try {
    const conn = await _getCalendlyToken(req.cliente_id);
    if (!conn) return res.json({ ok: true, info: 'No connection found' });

    // Delete webhook subscription on Calendly
    if (conn.webhook_uri && conn.access_token) {
      try {
        await _calendlyOAuth.deleteWebhookSubscription(conn.access_token, conn.webhook_uri);
        console.log(`[Calendly OAuth] Webhook deleted for negocio=${req.cliente_id}`);
      } catch (e) { console.warn('[Calendly OAuth] Could not delete webhook on Calendly:', e.message); }
    }

    // Remove from DB
    await supabase.from('calendly_connections').delete().eq('negocio_id', req.cliente_id);
    console.log(`[Calendly OAuth] Disconnected negocio=${req.cliente_id}`);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /calendly/debug  OR  GET /debug/calendly-last-events — full pipeline debug
async function _calendlyDebugHandler(req, res) {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  try {
    const [connectionsRes, mappingsRes, callsRes, dbLogRes] = await Promise.all([
      supabase.from('calendly_connections')
        .select('negocio_id, calendly_user_name, calendly_user_email, webhook_uri, webhook_token, connected_at')
        .order('connected_at', { ascending: false }),
      supabase.from('calendly_event_mappings')
        .select('*').order('created_at', { ascending: false }),
      supabase.from('calls')
        .select('id, nombre, email, estado, origen, cliente_id, fecha_llamada, calendly_invitee_uri, created_at')
        .eq('origen', 'Calendly')
        .order('created_at', { ascending: false }).limit(20),
      supabase.from('calendly_webhook_log')
        .select('*').order('received_at', { ascending: false }).limit(100),
    ]);
    res.json({
      connections:            connectionsRes.data || [],
      mappings:               mappingsRes.data    || [],
      recent_webhooks_memory: _calendlyWebhookLog,
      recent_webhooks_db:     dbLogRes.data       || [],
      recent_calls:           callsRes.data       || [],
      note:                   '"recent_webhooks_db" persists across deploys. "recent_webhooks_memory" resets on deploy.',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
app.get('/calendly/debug',               _calendlyDebugHandler);
app.get('/debug/calendly-last-events',   _calendlyDebugHandler);

// GET /calendly/mappings — list all mappings (holding access)
app.get('/calendly/mappings', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  const { data, error } = await supabase.from('calendly_event_mappings').select('*').order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// POST /calendly/mappings — add a mapping
app.post('/calendly/mappings', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  const { cliente_id, calendly_event_uri, label } = req.body;
  if (!cliente_id || !calendly_event_uri) return res.status(400).json({ error: 'Faltan cliente_id y calendly_event_uri' });
  const { data, error } = await supabase.from('calendly_event_mappings')
    .insert({ cliente_id, calendly_event_uri: calendly_event_uri.trim(), label: label || null })
    .select('*').single();
  if (error) return res.status(500).json({ error: error.message });
  console.log(`[Calendly] Mapping added: ${calendly_event_uri} → ${cliente_id}`);
  res.json(data);
});

// DELETE /calendly/mappings/:id — remove a mapping
app.delete('/calendly/mappings/:id', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  const { error } = await supabase.from('calendly_event_mappings').delete().eq('id', req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════
// 🏢  GHL INTEGRATION (GoHighLevel per-negocio)
// ══════════════════════════════════════════════════════

const _ghlProvider = require('./providers/ghl.provider');

// Determine which calendar provider a negocio uses.
// Set env var GHL_NEGOCIO_IDS="cliente_2,cliente_5" to add more.
function _getCalendarProvider(negocio_id) {
  const ghlIds = (process.env.GHL_NEGOCIO_IDS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
  return ghlIds.includes(negocio_id) ? 'ghl' : 'calendly';
}

// Async version: checks negocio_settings table first, then falls back to env var
async function _resolveCalendarProvider(negocio_id) {
  const { data } = await supabase
    .from('negocio_settings')
    .select('calendar_provider')
    .eq('negocio_id', negocio_id)
    .maybeSingle();
  if (data?.calendar_provider) return data.calendar_provider;
  return _getCalendarProvider(negocio_id);
}

// In-memory webhook log (last 50 events) for /ghl/debug
const _ghlWebhookLog = [];
function _logGhlWebhook(entry) {
  _ghlWebhookLog.unshift({ ...entry, at: new Date().toISOString() });
  if (_ghlWebhookLog.length > 50) _ghlWebhookLog.pop();
}

// Get + auto-refresh GHL access token for a negocio
async function _getGhlToken(negocio_id) {
  const { data: conn } = await supabase
    .from('calendar_integrations')
    .select('*')
    .eq('negocio_id', negocio_id)
    .eq('provider', 'ghl')
    .maybeSingle();
  if (!conn) return null;

  try {
    const result = await _ghlProvider.ensureFreshToken(conn);
    if (result.updated) {
      await supabase.from('calendar_integrations').update({
        access_token:     result.accessToken,
        refresh_token:    result.newRefresh,
        token_expires_at: result.newExpires,
        updated_at:       new Date().toISOString(),
      }).eq('negocio_id', negocio_id).eq('provider', 'ghl');
      console.log(`[GHL OAuth] Token refreshed for negocio=${negocio_id}`);
    }
    return { ...conn, access_token: result.accessToken };
  } catch (err) {
    console.error(`[GHL OAuth] Token refresh failed negocio=${negocio_id}:`, err.message);
    return conn;
  }
}

// Auto-update matching lead to 'Agendado' — same logic as Calendly version
async function _ghlUpdateLeadAgendado(instagram, name, cliente_id) {
  const FINAL = ['Cerrado', 'Seña', 'Perdido', 'Perdido Post Call', 'No Show'];
  let lead = null; let matchedBy = '';

  if (instagram) {
    const { data } = await supabase.from('leads').select('id, estado')
      .eq('cliente_id', cliente_id).ilike('instagram', instagram).limit(1).maybeSingle();
    if (data) { lead = data; matchedBy = 'instagram'; }
  }
  if (!lead && name && name !== 'Sin nombre') {
    const { data } = await supabase.from('leads').select('id, estado')
      .eq('cliente_id', cliente_id).ilike('nombre', name)
      .order('updated_at', { ascending: false }).limit(1).maybeSingle();
    if (data) { lead = data; matchedBy = 'nombre'; }
  }
  if (!lead) { console.log(`[GHL] No lead match for Agendado — ig="${instagram}" name="${name}"`); return; }
  if (FINAL.includes(lead.estado)) { console.log(`[GHL] Lead id=${lead.id} already "${lead.estado}" — skipping`); return; }

  const { error } = await supabase.from('leads')
    .update({ estado: 'Agendado', updated_at: new Date().toISOString() })
    .eq('id', lead.id).eq('cliente_id', cliente_id);
  if (error) console.warn(`[GHL] Lead Agendado update failed: ${error.message}`);
  else console.log(`[GHL] ✓ Lead id=${lead.id} → Agendado (matched by ${matchedBy})`);
}

// GHL sends appointment times in the calendar's local timezone WITHOUT an offset suffix.
// Without explicit timezone, PostgreSQL TIMESTAMPTZ treats bare ISO as UTC → 3-hour shift
// for America/Argentina/Buenos_Aires calendars (UTC-3, no DST since 2009).
function _normalizeGhlStartTime(raw) {
  if (!raw) return raw;
  const s = String(raw).trim();
  if (s.endsWith('Z') || /[+-]\d{2}:\d{2}$/.test(s) || /[+-]\d{4}$/.test(s)) return s;
  if (/^\d{10,13}$/.test(s)) return new Date(s.length === 13 ? +s : +s * 1000).toISOString();
  return s + '-03:00';
}

// Create or update a call in the `calls` table from a GHL appointment + contact
// rawPayload: original webhook body — used for nombre fallback, instagram scan, reporte_ghl
async function _ghlUpsertCall(appt, contact, cliente_id, eventType, rawPayload = {}) {
  // Normalize startTime — bare ISO from GHL has no timezone; append -03:00 so Supabase stores UTC correctly
  if (appt.startTime) appt = { ...appt, startTime: _normalizeGhlStartTime(appt.startTime) };

  // ── instagram (extracted first — needed to detect lastName-as-handle) ────────
  const instagram = _ghlProvider.extractInstagram(contact, rawPayload) || '';
  console.log(`[GHL Parser] resolved instagram=${instagram || '(none)'}`);

  // ── nombre: exclude lastName when it's an instagram handle or a multi-word phrase ───
  const _rawLastName = contact.lastName || contact.last_name || rawPayload.last_name || '';
  const _lastNameIsInstagram = !!(instagram && _rawLastName &&
    _rawLastName.replace(/^@/, '').replace(/\s+/g, '').toLowerCase() === instagram);
  // 4+ words → qualification answer mapped to lastName by mistake, not a real surname
  // "De la Cruz" (3 words) → preserved; "costos de mi negocio" (4 words) → excluded
  const _lastNameIsPhrase = _rawLastName.trim().split(/\s+/).length > 3;
  const nombre = [
    contact.firstName || contact.first_name || rawPayload.first_name || '',
    (_lastNameIsInstagram || _lastNameIsPhrase) ? '' : _rawLastName,
  ].filter(Boolean).join(' ').trim()
    || contact.full_name || rawPayload.full_name
    || contact.email     || 'Sin nombre';
  console.log(`[GHL Parser] resolved nombre="${nombre}" (lastNameIsInstagram=${_lastNameIsInstagram} lastNameIsPhrase=${_lastNameIsPhrase})`);

  const email    = contact.email || rawPayload.email || '';
  const telefono = contact.phone || contact.phone_raw || contact.full_phone_number
    || rawPayload.phone || rawPayload.phone_raw || rawPayload.contact?.phone || '';
  console.log(`[GHL Parser] resolved phone=${telefono || '(none)'}`);

  const UPDATE_TYPES = new Set(['AppointmentUpdate','AppointmentCancelled','AppointmentRescheduled','AppointmentConfirmed','AppointmentNoShow','AppointmentCompleted']);
  const isUpdate = UPDATE_TYPES.has(eventType);
  const isDelete = eventType === 'AppointmentDelete';
  const meetingLink = (appt.address && appt.address.startsWith('http')) ? appt.address : null;

  const apptId = appt.appointmentId || appt.id || null;

  // ── closer: body.user firstName+lastName → calendar_name fallback ────────────
  const closer = [
    rawPayload.user?.firstName || '',
    rawPayload.user?.lastName  || '',
  ].filter(Boolean).join(' ').trim() || '';
  const closerEmail = rawPayload.user?.email || '';
  console.log(`[GHL Parser] resolved closer="${closer || '(none)'}" closerEmail="${closerEmail || '(none)'}"`);

  // ── calendar_id: nested body.calendar.id → top-level variants ────────────────
  const calendarId = (rawPayload.calendar && (rawPayload.calendar.id || rawPayload.calendar.calendarId))
    || rawPayload.calendarId || rawPayload.calendar_id
    || appt.calendarId || appt.calendar_id || null;
  console.log(`[GHL Parser] resolved calendarId=${calendarId || '(none)'}`);

  // ── calendar_name: calendarName (human label) > name > title (appointment title) ──
  const calendarName = rawPayload.calendar?.calendarName
    || rawPayload.calendar?.name
    || appt.calendarName
    || appt.calendarTitle
    || appt.title
    || null;
  console.log(`[GHL Parser] resolved calendarName="${calendarName || '(none)'}"`);

  // ── estado: event-type override > appointmentStatus ────────────────────────
  // GHL typos "appointmentStatus" as "appoinmentStatus" (missing 't') in calendar object
  const estadoByEvent = _ghlProvider.mapEventTypeToEstado(eventType);
  const apptStatus = appt.appointmentStatus || appt.appoinmentStatus || '';
  const estado = estadoByEvent || _ghlProvider.mapAppointmentStatus(apptStatus);

  // Extract qualification answers early so they're available for both UPDATE and INSERT
  const qualAnswers  = _ghlProvider.extractQualificationAnswers(rawPayload);
  const answerCount  = Object.keys(qualAnswers).length;
  console.log(`[GHL Parser] qualification answers extracted=${answerCount}`);
  if (answerCount > 0) console.log(`[GHL Parser] qualification keys: ${Object.keys(qualAnswers).join(' | ')}`);
  const preguntasCalificacion = answerCount > 0 ? JSON.stringify(qualAnswers) : null;

  console.log(`[GHL UpsertCall] nombre="${nombre}" email=${email} phone=${telefono} apptId=${apptId} estado=${estado} startTime=${appt.startTime || '—'}`);

  // Try to find existing call by provider_event_id
  if (apptId) {
    const { data: existing, error: lookupErr } = await supabase.from('calls')
      .select('id, estado, fecha_llamada, instagram, nombre, whatsapp, email, preguntas_calificacion, closer, calendar_id')
      .eq('cliente_id', cliente_id).eq('provider_event_id', apptId).maybeSingle();
    if (lookupErr) console.warn(`[GHL UpsertCall] Lookup error: ${lookupErr.message}`);

    if (existing) {
      console.log(`[GHL UpsertCall] Found existing call id=${existing.id} — updating (event=${eventType})`);
      const updatePatch = {
        fecha_llamada: appt.startTime || null,
        estado:        isDelete ? 'Cancelada' : estado,
        ...(meetingLink && { link_llamada: meetingLink }),
        // Enrich: only overwrite if new value is non-empty, otherwise keep existing
        ...(nombre && nombre !== 'Sin nombre'           && { nombre }),
        ...(instagram || existing.instagram             ? { instagram: instagram || existing.instagram } : {}),
        ...(telefono                                    && { whatsapp: telefono }),
        ...(email                                       && { email }),
        ...(preguntasCalificacion || existing.preguntas_calificacion
          ? { preguntas_calificacion: preguntasCalificacion || existing.preguntas_calificacion } : {}),
        ...(apptId                                      && { provider_event_id: apptId }),
        ...(calendarName                                && { calendar_name: calendarName }),
        ...(closer || existing.closer                   ? { closer: closer || existing.closer } : {}),
        ...(calendarId || existing.calendar_id          ? { calendar_id: calendarId || existing.calendar_id } : {}),
      };
      if (eventType === 'AppointmentRescheduled' && appt.startTime && appt.startTime !== existing.fecha_llamada) {
        updatePatch.reagendada = true;
      }

      console.log(`[GHL UpsertCall] Supabase UPDATE calls id=${existing.id} patch=${JSON.stringify(updatePatch)}`);
      let { error } = await supabase.from('calls')
        .update(updatePatch).eq('id', existing.id).eq('cliente_id', cliente_id);

      if (error && error.message?.includes('reagendada')) {
        delete updatePatch.reagendada;
        console.log(`[GHL UpsertCall] Retrying without reagendada field`);
        ({ error } = await supabase.from('calls')
          .update(updatePatch).eq('id', existing.id).eq('cliente_id', cliente_id));
      }
      if (error) {
        console.error(`[GHL UpsertCall] ❌ UPDATE failed: ${error.message} | code=${error.code} | details=${error.details}`);
        throw new Error(`[GHL] Update call failed: ${error.message}`);
      }
      console.log(`[GHL UpsertCall] ✓ Updated call id=${existing.id} event=${eventType} estado=${updatePatch.estado}`);
      return existing.id;
    }
  }

  if (isDelete || estado === 'Cancelada') {
    console.log(`[GHL UpsertCall] ${eventType} (estado=${estado}) — no existing call for apptId=${apptId}, skipping INSERT`);
    return null;
  }

  // ── Reschedule detection ──────────────────────────────────────────────────
  // GHL has no native rescheduled event — it cancels the old appointment and
  // creates a new one with a different appointmentId. Detect reschedules by
  // finding an existing pending GHL call from the same contact.
  // Match: instagram (primary) → email (fallback). Never match by name.
  if (eventType === 'AppointmentCreate') {
    const ACTIVE_ESTADOS = ['Pendiente', 'Agendado', 'Re agenda', 'Cancelada'];
    let rescheduleMatches = null;

    // Only match calls with a future fecha_llamada — past appointments cannot be rescheduled
    const nowIso = new Date().toISOString();

    if (instagram) {
      const { data } = await supabase.from('calls')
        .select('id, fecha_llamada, provider_event_id')
        .eq('cliente_id', cliente_id).eq('origen', 'GHL').eq('instagram', instagram)
        .in('estado', ACTIVE_ESTADOS)
        .gte('fecha_llamada', nowIso);
      rescheduleMatches = data;
      console.log(`[GHL Reschedule] instagram="${instagram}" future active matches=${rescheduleMatches?.length ?? 0}`);
    } else if (email) {
      const { data } = await supabase.from('calls')
        .select('id, fecha_llamada, provider_event_id')
        .eq('cliente_id', cliente_id).eq('origen', 'GHL').eq('email', email)
        .in('estado', ACTIVE_ESTADOS)
        .gte('fecha_llamada', nowIso);
      rescheduleMatches = data;
      console.log(`[GHL Reschedule] email="${email}" future active matches=${rescheduleMatches?.length ?? 0}`);
    } else {
      console.log(`[GHL Reschedule] No instagram or email — skipping reschedule detection`);
    }

    if (rescheduleMatches?.length === 1) {
      const target = rescheduleMatches[0];
      console.log(`[GHL Reschedule] Exactly 1 active call found id=${target.id} — updating as reagenda`);
      const patch = {
        estado:            'Re agenda',
        reagendada:        true,
        fecha_llamada:     appt.startTime   || null,
        provider_event_id: apptId           || null,
        ...(closer       && { closer }),
        ...(calendarName && { calendar_name: calendarName }),
        ...(meetingLink  && { link_llamada: meetingLink }),
        ...(preguntasCalificacion && { preguntas_calificacion: preguntasCalificacion }),
      };
      const { error: reErr } = await supabase.from('calls')
        .update(patch).eq('id', target.id).eq('cliente_id', cliente_id);
      if (reErr) {
        console.error(`[GHL Reschedule] ❌ Update failed: ${reErr.message} — falling through to INSERT`);
      } else {
        console.log(`[GHL Reschedule] ✓ id=${target.id} → "Re agenda" apptId=${apptId} newTime=${appt.startTime}`);
        return target.id;
      }
    } else if (rescheduleMatches?.length > 1) {
      console.log(`[GHL Reschedule] ${rescheduleMatches.length} active calls found — creating new call`);
    }
    // length === 0 or null: fall through to normal INSERT
  }

  // Count existing calls for this instagram to set numero_llamada
  let numero_llamada = 1;
  if (instagram) {
    const { data: prev } = await supabase.from('calls').select('id')
      .eq('instagram', instagram).eq('cliente_id', cliente_id);
    numero_llamada = (prev?.length || 0) + 1;
  }

  const fullRow = {
    cliente_id,
    nombre,
    instagram,
    whatsapp:               telefono,
    email,
    origen:                 'GHL',
    estado,
    numero_llamada,
    seguimientos:           0,
    responde:               false,
    fecha_llamada:          appt.startTime  || null,
    link_llamada:           meetingLink     || null,
    provider_event_id:      apptId         || null,
    calendar_name:          calendarName   || null,
    preguntas_calificacion: preguntasCalificacion,
    closer:                 closer         || null,
    calendar_id:            calendarId     || null,
  };

  console.log(`[GHL UpsertCall] Supabase INSERT calls: ${JSON.stringify(fullRow)}`);
  let { data, error } = await supabase.from('calls').insert(fullRow).select('id').single();

  if (error) {
    console.error(`[GHL UpsertCall] ❌ Full INSERT failed: ${error.message} | code=${error.code} | details=${error.details}`);
    console.warn(`[GHL UpsertCall] Retrying with core fields only`);
    const coreRow = {
      cliente_id, nombre, instagram, whatsapp: telefono, email,
      origen: 'GHL', estado, numero_llamada, seguimientos: 0, responde: false,
      fecha_llamada: appt.startTime || null, link_llamada: meetingLink || null,
      closer: closer || null, calendar_name: calendarName || null,
      provider_event_id: apptId || null, calendar_id: calendarId || null,
    };
    console.log(`[GHL UpsertCall] Core INSERT: ${JSON.stringify(coreRow)}`);
    ({ data, error } = await supabase.from('calls').insert(coreRow).select('id').single());
    if (error) {
      console.error(`[GHL UpsertCall] ❌ Core INSERT also failed: ${error.message} | code=${error.code} | details=${error.details}`);
      throw new Error(`[GHL] Core insert also failed: ${error.message}`);
    }
  }

  console.log(`[GHL UpsertCall] ✓ INSERT OK — call id=${data.id} nombre="${nombre}" email=${email} fecha=${appt.startTime}`);

  await _ghlUpdateLeadAgendado(instagram, nombre, cliente_id).catch(e => {
    console.warn('[GHL] Lead Agendado update error:', e.message);
  });

  return data.id;
}

// Save raw GHL payload to calls_ghl table for cliente_2 audit trail
async function _ghlSaveRawPayload({ cliente_id, call_id, eventType, inferred, rawBody, contact, calendar }) {
  try {
    const c = contact || {};
    const row = {
      cliente_id,
      call_id:            call_id    || null,
      contact_id:         c.id       || rawBody.contactId || rawBody.contact_id || null,
      first_name:         c.firstName || null,
      last_name:          c.lastName  || null,
      full_name:          [c.firstName, c.lastName].filter(Boolean).join(' ') || c.name || null,
      email:              c.email     || null,
      phone:              c.phone     || null,
      calendar:           calendar   ? JSON.stringify(calendar)                          : null,
      workflow:           rawBody.workflow          ? JSON.stringify(rawBody.workflow)          : null,
      trigger_data:       rawBody.triggerData       ? JSON.stringify(rawBody.triggerData)       : null,
      location:           rawBody.location          ? JSON.stringify(rawBody.location)          : null,
      attribution_source: rawBody.attributionSource ? JSON.stringify(rawBody.attributionSource) : null,
      custom_data:        rawBody.customData        ? JSON.stringify(rawBody.customData)        : null,
      raw_payload:        JSON.stringify(rawBody),
      event_type:         eventType  || null,
      inferred:           !!inferred,
    };
    const { error } = await supabase.from('calls_ghl').insert(row);
    if (error) console.warn(`[GHL RawLog] Insert to calls_ghl failed: ${error.message} | code=${error.code}`);
    else console.log(`[GHL RawLog] ✓ Raw payload saved to calls_ghl (call_id=${call_id || 'pending'})`);
  } catch (err) {
    console.warn(`[GHL RawLog] Unexpected error: ${err.message}`);
  }
}

// POST /webhooks/ghl  (also /api/ghl/webhook) — receives GHL appointment events
app.post(['/webhooks/ghl', '/api/ghl/webhook'], async (req, res) => {
  const ts = new Date().toISOString();
  console.log(`\n========== GHL WEBHOOK RECEIVED [${ts}] ==========`);
  console.log(`[GHL Webhook] method=${req.method} path=${req.path} url=${req.originalUrl}`);
  console.log(`[GHL Webhook] headers: ${JSON.stringify({
    'content-type':  req.headers['content-type'],
    'user-agent':    req.headers['user-agent'],
    'x-ghl-signature': req.headers['x-ghl-signature'] || '(none)',
  })}`);
  console.log(`[GHL Webhook] body: ${JSON.stringify(req.body || {})}`);
  console.log(`==================================================`);

  try {
    const webhookToken = req.query.t;
    const rawBody      = req.body || {};

    // Normalize payload — handles GHL v1/v2/workflow field name variations
    const { type: eventType, inferred, appointmentId, contactId, locationId, embeddedContact, embeddedCalendar } =
      _ghlProvider.normalizeWebhookPayload(rawBody);

    console.log(`[GHL Webhook] Parsed → eventType=${eventType || 'NONE'} inferred=${inferred} appointmentId=${appointmentId || '?'} contactId=${contactId || '?'} locationId=${locationId || '?'}`);
    if (inferred) console.log(`[GHL Webhook] inferred eventType=AppointmentCreate (fallback)`);
    console.log(`[GHL Webhook] Embedded objects → contact=${embeddedContact ? 'YES' : 'no'} calendar=${embeddedCalendar ? 'YES' : 'no'}`);
    console.log(`[GHL Webhook] Location sources → body.locationId=${rawBody.locationId || '—'} body.location?.id=${rawBody.location?.id || '—'} → resolved=${locationId || 'NONE'}`);

    // Identify negocio from webhook token (?t=TOKEN in URL)
    let cliente_id = null;
    if (webhookToken) {
      const { data: conn, error: dbErr } = await supabase
        .from('calendar_integrations').select('negocio_id')
        .eq('webhook_token', webhookToken).eq('provider', 'ghl').maybeSingle();
      if (dbErr) console.warn(`[GHL Webhook] DB lookup by token error: ${dbErr.message}`);
      if (conn) {
        cliente_id = conn.negocio_id;
        console.log(`[GHL Webhook] ✓ Negocio identified by token → cliente_id=${cliente_id}`);
      } else {
        console.log(`[GHL Webhook] Token not found in DB (token=${webhookToken})`);
      }
    }

    // Fallback 2: identify by locationId in payload (DB lookup)
    if (!cliente_id && locationId) {
      const { data: conn, error: dbErr } = await supabase
        .from('calendar_integrations').select('negocio_id')
        .eq('provider_location_id', locationId).eq('provider', 'ghl').maybeSingle();
      if (dbErr) console.warn(`[GHL Webhook] DB lookup by locationId error: ${dbErr.message}`);
      if (conn) {
        cliente_id = conn.negocio_id;
        console.log(`[GHL Webhook] ✓ Negocio identified by locationId=${locationId} → cliente_id=${cliente_id}`);
      } else {
        console.log(`[GHL Webhook] locationId not found in DB (locationId=${locationId})`);
      }
    }

    // Fallback 3: match by GHL_LOCATION_ID env var (Private Integration — no DB row needed)
    if (!cliente_id && locationId) {
      const envLocationId = process.env.GHL_LOCATION_ID;
      console.log(`[GHL Webhook] Env fallback check: GHL_LOCATION_ID=${envLocationId || '(not set)'} vs payload locationId=${locationId}`);
      if (envLocationId === locationId) {
        const ghlIds = (process.env.GHL_NEGOCIO_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
        console.log(`[GHL Webhook] GHL_NEGOCIO_IDS=${ghlIds.join(',') || '(not set)'}`);
        if (ghlIds.length > 0) {
          cliente_id = ghlIds[0];
          console.log(`[GHL Webhook] ✓ Negocio identified by GHL_LOCATION_ID env var → cliente_id=${cliente_id}`);
        }
      }
    }

    if (!cliente_id) {
      console.warn(`[GHL Webhook] ❌ Could not identify negocio`);
      console.warn(`[GHL Webhook] Debug: webhookToken=${webhookToken || 'none'}, locationId=${locationId || 'none'}`);
      console.warn(`[GHL Webhook] Debug: GHL_LOCATION_ID=${process.env.GHL_LOCATION_ID || '(not set)'}, GHL_NEGOCIO_IDS=${process.env.GHL_NEGOCIO_IDS || '(not set)'}`);
      _logGhlWebhook({ eventType, status: 'no_mapping', locationId });
      return res.json({ ok: true, warning: 'No mapping found — event logged but no call created' });
    }

    if (!eventType) {
      // No type and no calendar+contact to infer from — log and accept (don't 400, GHL expects 200)
      console.warn(`[GHL Webhook] ⚠ Could not determine eventType — body keys: ${Object.keys(rawBody).join(', ')}`);
      await _ghlSaveRawPayload({ cliente_id, call_id: null, eventType: null, inferred: false, rawBody, contact: null, calendar: null });
      return res.json({ ok: true, info: 'Event received but type could not be determined — saved to calls_ghl for review' });
    }

    console.log(`[GHL Webhook] Processing eventType=${eventType}${inferred ? ' (inferred)' : ''} for cliente_id=${cliente_id}`);

    if (!['AppointmentCreate', 'AppointmentUpdate', 'AppointmentDelete', 'AppointmentCancelled', 'AppointmentRescheduled'].includes(eventType)) {
      console.log(`[GHL Webhook] Skipping unhandled event type: ${eventType}`);
      await _ghlSaveRawPayload({ cliente_id, call_id: null, eventType, inferred, rawBody, contact: null, calendar: null });
      return res.json({ ok: true, info: `Unhandled: ${eventType}` });
    }

    _logGhlWebhook({ eventType, inferred, appointmentId, contactId, cliente_id, status: 'processing' });

    // AppointmentDelete — mark existing call Cancelada (no API token needed)
    if (eventType === 'AppointmentDelete') {
      if (appointmentId) {
        console.log(`[GHL Webhook] Supabase UPDATE calls SET estado=Cancelada WHERE provider_event_id=${appointmentId}`);
        const { data: updated, error } = await supabase.from('calls')
          .update({ estado: 'Cancelada' })
          .eq('cliente_id', cliente_id).eq('provider_event_id', appointmentId).select('id');
        if (error) {
          console.error(`[GHL Webhook] ❌ Delete update failed: ${error.message} | code=${error.code}`);
        } else {
          console.log(`[GHL Webhook] ✓ AppointmentDelete → Cancelada rows=${updated?.length || 0} apptId=${appointmentId}`);
        }
      }
      await _ghlSaveRawPayload({ cliente_id, call_id: null, eventType, inferred, rawBody, contact: null, calendar: embeddedCalendar });
      _logGhlWebhook({ eventType, appointmentId, cliente_id, status: 'deleted' });
      return res.json({ ok: true });
    }

    // ── Build contact object ──────────────────────────────────────────────────
    // Priority: embedded contact from payload > GHL API call > raw payload fields
    let contact = {
      id:        contactId || null,
      firstName: rawBody.firstName || rawBody.name || '',
      lastName:  rawBody.lastName  || '',
      email:     rawBody.email     || '',
      phone:     rawBody.phone     || '',
    };

    if (embeddedContact) {
      // Merge: embeddedContact often only has attribution data — fill name/phone/email from rawBody if missing
      contact = {
        ...embeddedContact,
        firstName: embeddedContact.firstName || embeddedContact.first_name || rawBody.first_name || rawBody.firstName || '',
        lastName:  embeddedContact.lastName  || embeddedContact.last_name  || rawBody.last_name  || rawBody.lastName  || '',
        full_name: embeddedContact.full_name || rawBody.full_name  || '',
        email:     embeddedContact.email     || rawBody.email              || '',
        phone:     embeddedContact.phone     || rawBody.phone              || '',
      };
      const fullName = [contact.firstName, contact.lastName].filter(Boolean).join(' ') || contact.full_name || '(no name)';
      console.log(`[GHL Webhook] Contact source: EMBEDDED (merged) → name="${fullName}" email=${contact.email || '—'} phone=${contact.phone || '—'}`);

      // If still no name after merge, enrich via GHL API (embeddedContact may lack name fields)
      const hasName = contact.firstName || contact.lastName || contact.full_name;
      if (!hasName && contactId) {
        const conn     = await _getGhlToken(cliente_id);
        const apiToken = conn?.access_token || process.env.GHL_API_KEY || null;
        if (apiToken) {
          try {
            const apiContact = await _ghlProvider.getContact(apiToken, contactId);
            contact.firstName = apiContact.firstName || apiContact.first_name || contact.firstName;
            contact.lastName  = apiContact.lastName  || apiContact.last_name  || contact.lastName;
            contact.full_name = apiContact.full_name || contact.full_name;
            if (!contact.email) contact.email = apiContact.email || '';
            if (!contact.phone) contact.phone = apiContact.phone || '';
            const n = [contact.firstName, contact.lastName].filter(Boolean).join(' ') || contact.full_name;
            console.log(`[GHL Webhook] Contact enriched via API (embedded had no name) → name="${n}"`);
          } catch (e) {
            console.warn(`[GHL Webhook] ⚠ API name enrichment failed: ${e.message}`);
          }
        }
      }
    } else {
      const conn     = await _getGhlToken(cliente_id);
      const apiToken = conn?.access_token || process.env.GHL_API_KEY || null;
      console.log(`[GHL Webhook] API token source: ${conn ? 'DB' : apiToken ? 'GHL_API_KEY env' : 'NONE'}`);
      if (contactId && apiToken) {
        console.log(`[GHL Webhook] Contact source: GHL API (id=${contactId})`);
        try {
          contact = await _ghlProvider.getContact(apiToken, contactId);
          const fullName = [contact.firstName, contact.lastName].filter(Boolean).join(' ') || '(no name)';
          console.log(`[GHL Webhook] ✓ Contact fetched: name="${fullName}" email=${contact.email || '—'} phone=${contact.phone || '—'}`);
        } catch (e) {
          console.warn(`[GHL Webhook] ⚠ Contact API fetch failed: ${e.message} — using payload fields`);
        }
      } else {
        console.log(`[GHL Webhook] Contact source: PAYLOAD fields → firstName=${contact.firstName || '—'} email=${contact.email || '—'}`);
      }
    }

    // Enrich contact with top-level payload fields (GHL sometimes sends first_name/last_name at root)
    if (rawBody.first_name || rawBody.last_name) {
      contact = {
        ...contact,
        firstName: contact.firstName || rawBody.first_name || '',
        lastName:  contact.lastName  || rawBody.last_name  || '',
      };
      console.log(`[GHL Webhook] Contact enriched with top-level name fields: "${contact.firstName} ${contact.lastName}".trim()`);
    }

    // ── Build appointment payload ─────────────────────────────────────────────
    // Priority: embedded calendar object > raw body
    let apptPayload;
    if (embeddedCalendar) {
      apptPayload = {
        ...embeddedCalendar,
        appointmentId: embeddedCalendar.appointmentId || embeddedCalendar.id || appointmentId,
        id:            embeddedCalendar.appointmentId || embeddedCalendar.id || appointmentId,
      };
      console.log(`[GHL Webhook] Appt source: EMBEDDED calendar → id=${apptPayload.id} title="${apptPayload.title || '—'}" startTime=${apptPayload.startTime || '—'} status=${apptPayload.appointmentStatus || '—'}`);
    } else {
      apptPayload = { ...rawBody, appointmentId, id: appointmentId };
      console.log(`[GHL Webhook] Appt source: RAW body → apptId=${appointmentId}`);
    }

    // ── Upsert into calls table ───────────────────────────────────────────────
    console.log(`[GHL Webhook] → _ghlUpsertCall eventType=${eventType} apptId=${apptPayload.id} cliente_id=${cliente_id}`);
    const callId = await _ghlUpsertCall(apptPayload, contact, cliente_id, eventType, rawBody);

    // ── Save raw payload to calls_ghl (audit trail for cliente_2) ────────────
    await _ghlSaveRawPayload({ cliente_id, call_id: callId, eventType, inferred, rawBody, contact, calendar: embeddedCalendar });

    // ── Update webhook health timestamp in calendar_integrations ─────────────
    try {
      const { data: connRow } = await supabase.from('calendar_integrations')
        .select('metadata').eq('negocio_id', cliente_id).eq('provider', 'ghl').maybeSingle();
      const prevMeta = (typeof connRow?.metadata === 'string'
        ? JSON.parse(connRow.metadata) : connRow?.metadata) || {};
      await supabase.from('calendar_integrations')
        .update({ metadata: { ...prevMeta, last_webhook_at: new Date().toISOString() } })
        .eq('negocio_id', cliente_id).eq('provider', 'ghl');
      console.log(`[GHL Health] ✓ last_webhook_at updated for cliente_id=${cliente_id}`);
    } catch (e) {
      console.warn(`[GHL Health] Could not update last_webhook_at: ${e.message}`);
    }

    _logGhlWebhook({ eventType, inferred, appointmentId, contactId, cliente_id, callId,
      status: eventType === 'AppointmentUpdate' ? 'updated' : 'created' });

    console.log(`[GHL Webhook] ✅ Done — callId=${callId} eventType=${eventType}${inferred ? ' (inferred)' : ''}`);
    res.json({ ok: true, call_id: callId });

  } catch (err) {
    console.error(`[GHL Webhook] ❌ UNHANDLED ERROR: ${err.message}`);
    console.error(`[GHL Webhook] Stack: ${err.stack}`);
    _logGhlWebhook({ eventType: req.body?.type, status: 'error', error: err.message });
    res.status(500).json({ error: err.message });
  }
});

// GET /ghl/connect?cliente_id=X — redirect to GHL OAuth authorization
app.get('/ghl/connect', (req, res) => {
  const { cliente_id } = req.query;
  if (!cliente_id)                   return res.status(400).send('Missing cliente_id');
  if (!process.env.GHL_CLIENT_ID)    return res.status(503).send('GHL_CLIENT_ID not configured');

  const backendUrl  = process.env.BACKEND_URL || `${req.protocol}://${req.get('host')}`;
  const redirectUri = `${backendUrl}/oauth/callback`;
  const state       = Buffer.from(JSON.stringify({ cliente_id, nonce: require('crypto').randomBytes(8).toString('hex') })).toString('base64url');

  const authUrl = _ghlProvider.buildOAuthURL(redirectUri, state);
  console.log(`[GHL OAuth] Redirecting negocio=${cliente_id} to GHL auth`);
  res.redirect(authUrl);
});

// GET /oauth/callback — OAuth callback from GHL (GHL disallows "ghl" in redirect URI)
app.get('/oauth/callback', async (req, res) => {
  const { code, state, error: oauthError } = req.query;

  if (oauthError) {
    console.warn('[GHL OAuth] User denied access:', oauthError);
    return res.send(_ghlPopupPage(false, null, `Acceso denegado: ${oauthError}`));
  }
  if (!code || !state) return res.status(400).send('Missing code or state');

  let cliente_id;
  try {
    const decoded = JSON.parse(Buffer.from(state, 'base64url').toString());
    cliente_id    = decoded.cliente_id;
    if (!cliente_id) throw new Error('No cliente_id in state');
  } catch (err) {
    return res.status(400).send('Invalid state parameter');
  }

  try {
    const backendUrl  = process.env.BACKEND_URL || `${req.protocol}://${req.get('host')}`;
    const redirectUri = `${backendUrl}/oauth/callback`;

    console.log(`[GHL OAuth] Exchanging code for tokens — negocio=${cliente_id}`);
    const tokens     = await _ghlProvider.exchangeCode(code, redirectUri);
    const expiresAt  = new Date(Date.now() + (tokens.expires_in || 86400) * 1000).toISOString();
    const locationId = tokens.locationId || null;
    const userId     = tokens.userId     || null;

    console.log(`[GHL OAuth] Tokens received — locationId=${locationId} userId=${userId}`);

    // Fetch location name for display
    let locationName = null;
    if (locationId && tokens.access_token) {
      try {
        const loc  = await _ghlProvider.getLocation(tokens.access_token, locationId);
        locationName = loc?.name || null;
        console.log(`[GHL OAuth] Location name: "${locationName}"`);
      } catch (e) { console.warn('[GHL OAuth] Could not fetch location:', e.message); }
    }

    // Generate per-negocio webhook token for URL-based identification
    const webhookToken = _ghlProvider.generateWebhookToken();
    const webhookUrl   = `${backendUrl}/webhooks/ghl?t=${webhookToken}`;

    // Remove old webhook subscription if any
    const { data: existing } = await supabase.from('calendar_integrations')
      .select('webhook_id, access_token').eq('negocio_id', cliente_id).eq('provider', 'ghl').maybeSingle();
    if (existing?.webhook_id && existing?.access_token) {
      try {
        await _ghlProvider.deleteWebhookSubscription(existing.access_token, existing.webhook_id);
        console.log(`[GHL OAuth] Deleted old webhook ${existing.webhook_id}`);
      } catch (e) { console.warn('[GHL OAuth] Could not delete old webhook:', e.message); }
    }

    // Create new webhook subscription
    let webhookId = null;
    if (locationId) {
      try {
        const wh = await _ghlProvider.createWebhookSubscription(tokens.access_token, locationId, webhookUrl);
        webhookId = wh?.id || wh?.webhookId || null;
        console.log(`[GHL OAuth] ✓ Webhook auto-created id=${webhookId} → ${webhookUrl}`);
      } catch (e) {
        console.warn('[GHL OAuth] ⚠ Webhook auto-creation failed:', e.message);
        console.warn(`[GHL OAuth] ▶ Set webhook MANUALLY in GHL → Sub-account → Settings → Integrations → Webhooks`);
        console.warn(`[GHL OAuth] ▶ Webhook URL to paste: ${webhookUrl}`);
        console.warn(`[GHL OAuth] ▶ Events to select: AppointmentCreate, AppointmentUpdate, AppointmentDelete`);
      }
    }

    // Upsert into calendar_integrations
    const connRow = {
      negocio_id:           cliente_id,
      provider:             'ghl',
      access_token:         tokens.access_token,
      refresh_token:        tokens.refresh_token  || null,
      token_expires_at:     expiresAt,
      provider_user_id:     userId,
      provider_location_id: locationId,
      webhook_id:           webhookId,
      webhook_token:        webhookToken,
      webhook_url:          webhookUrl,
      metadata:             JSON.stringify({ locationName }),
      connected_at:         new Date().toISOString(),
      status:               'connected',
      updated_at:           new Date().toISOString(),
    };

    const { error: upsertErr } = await supabase
      .from('calendar_integrations').upsert(connRow, { onConflict: 'negocio_id' });
    if (upsertErr) throw new Error(`DB upsert failed: ${upsertErr.message}`);

    // Auto-save provider preference so the holding dashboard knows this negocio uses GHL
    await supabase.from('negocio_settings')
      .upsert({ negocio_id: cliente_id, calendar_provider: 'ghl' }, { onConflict: 'negocio_id' });

    console.log(`[GHL OAuth] ✓ Connection saved — negocio=${cliente_id} location=${locationId}`);
    res.send(_ghlPopupPage(true, cliente_id, null));

  } catch (err) {
    console.error('[GHL OAuth] Callback error:', err.message);
    res.send(_ghlPopupPage(false, cliente_id, err.message));
  }
});

function _ghlPopupPage(success, cliente_id, errorMsg) {
  const msg = success
    ? `{type:'ghl_connected', cliente_id:${JSON.stringify(cliente_id)}}`
    : `{type:'ghl_error', error:${JSON.stringify(errorMsg || 'Unknown error')}}`;
  return `<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{font-family:sans-serif;background:#0d0e12;color:#9ba0b4;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}</style>
</head><body>
<div style="text-align:center">
  <div style="font-size:32px;margin-bottom:12px">${success ? '✅' : '❌'}</div>
  <div style="font-size:14px">${success ? 'GoHighLevel conectado. Cerrando...' : 'Error: ' + (errorMsg || '')}</div>
</div>
<script>
  try { window.opener && window.opener.postMessage(${msg}, '*'); } catch(e) {}
  setTimeout(() => window.close(), 1500);
</script>
</body></html>`;
}

// GET /ghl/connection-status — connection info for the current negocio
app.get('/ghl/connection-status', validateAccess, async (req, res) => {
  const { data, error } = await supabase
    .from('calendar_integrations')
    .select('negocio_id, provider, provider_location_id, webhook_url, connected_at, status, metadata')
    .eq('negocio_id', req.cliente_id).eq('provider', 'ghl').maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || null);
});

// DELETE /ghl/disconnect — disconnect GHL from the current negocio
app.delete('/ghl/disconnect', validateAccess, async (req, res) => {
  try {
    const conn = await _getGhlToken(req.cliente_id);
    if (!conn) return res.json({ ok: true, info: 'No connection found' });

    if (conn.webhook_id) {
      try {
        await _ghlProvider.deleteWebhookSubscription(conn.access_token, conn.webhook_id);
        console.log(`[GHL OAuth] Webhook deleted for negocio=${req.cliente_id}`);
      } catch (e) { console.warn('[GHL OAuth] Could not delete webhook on GHL:', e.message); }
    }

    await supabase.from('calendar_integrations').delete()
      .eq('negocio_id', req.cliente_id).eq('provider', 'ghl');
    console.log(`[GHL OAuth] Disconnected negocio=${req.cliente_id}`);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /integration/provider — returns which calendar provider this negocio uses + connection status
// Used by the frontend "Integraciones" page
app.get('/integration/provider', validateAccess, async (req, res) => {
  const provider = await _resolveCalendarProvider(req.cliente_id);
  let connected  = false;
  let info       = null;

  if (provider === 'ghl') {
    const { data } = await supabase
      .from('calendar_integrations')
      .select('connected_at, status, metadata, provider_location_id, webhook_url, webhook_id')
      .eq('negocio_id', req.cliente_id).eq('provider', 'ghl').maybeSingle();

    // Private Integration fallback: if DB has placeholder or no row, use env vars
    const PLACEHOLDER = 'TU_LOCATION_ID_AQUI';
    const envLocId    = process.env.GHL_LOCATION_ID;
    const envKey      = process.env.GHL_API_KEY;
    const envNegocio  = process.env.GHL_NEGOCIO_IDS; // e.g. "cliente_2"

    let locationId = data?.provider_location_id;
    if (!locationId || locationId === PLACEHOLDER) locationId = envLocId || locationId;

    if (data) {
      connected = data.status === 'connected';
      info      = { ...data, provider_location_id: locationId };
    } else if (envKey && envLocId && envNegocio && envNegocio.split(',').map(s=>s.trim()).includes(req.cliente_id)) {
      // Private Integration without a DB row — synthesize connected state
      connected = true;
      info = {
        provider_location_id: envLocId,
        connected_at: null,
        status: 'connected',
        webhook_id: null,
        webhook_url: null,
        metadata: null,
      };
    }

    // Enrich with calendars and closers/setters detected from GHL calls
    if (connected && info) {
      const [calsRes, closersRes] = await Promise.all([
        supabase.from('calls').select('calendar_name').eq('cliente_id', req.cliente_id).eq('origen', 'GHL').not('calendar_name', 'is', null).limit(200),
        supabase.from('calls').select('closer').eq('cliente_id', req.cliente_id).eq('origen', 'GHL').not('closer', 'is', null).neq('closer', '').limit(200),
      ]);
      info.detected_calendars = [...new Set((calsRes.data  || []).map(r => r.calendar_name).filter(Boolean))];
      info.detected_closers   = [...new Set((closersRes.data || []).map(r => r.closer).filter(Boolean))];
    }
  } else {
    const { data } = await supabase
      .from('calendly_connections')
      .select('connected_at, calendly_user_name, calendly_user_email, webhook_uri')
      .eq('negocio_id', req.cliente_id).maybeSingle();
    connected = !!data;
    info      = data;
  }

  res.json({ provider, connected, info });
});

// POST /integration/set-provider — holding-only: set preferred calendar provider for a negocio
app.post('/integration/set-provider', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  const { negocio_id, provider } = req.body;
  if (!negocio_id || !['ghl', 'calendly'].includes(provider))
    return res.status(400).json({ error: 'negocio_id y provider (ghl|calendly) son requeridos' });
  const { error } = await supabase.from('negocio_settings')
    .upsert({ negocio_id, calendar_provider: provider }, { onConflict: 'negocio_id' });
  if (error) return res.status(500).json({ error: error.message });
  console.log(`[Integration] Provider set: negocio=${negocio_id} provider=${provider}`);
  res.json({ ok: true, negocio_id, provider });
});

// POST /ghl/register-native-webhook — registers GHL native webhook using stored OAuth token
// Needed when the OAuth flow didn't auto-create the subscription (e.g. locationId was missing)
app.post('/ghl/register-native-webhook', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });

  const { cliente_id } = req.body;
  if (!cliente_id) return res.status(400).json({ error: 'Missing cliente_id' });

  try {
    const conn = await _getGhlToken(cliente_id);
    if (!conn) return res.status(404).json({ error: 'GHL connection not found for this cliente_id' });

    const accessToken = conn.access_token;
    if (!accessToken) return res.status(500).json({ error: 'No access token available — reconnect GHL' });

    // locationId: prefer DB row (multi-tenant), fall back to env var (cliente_2 legacy)
    const locationId = conn.provider_location_id || process.env.GHL_LOCATION_ID;
    if (!locationId) return res.status(500).json({ error: 'No location ID — set provider_location_id in DB or GHL_LOCATION_ID env var' });

    const backendUrl = process.env.BACKEND_URL || `${req.protocol}://${req.get('host')}`;
    const webhookToken = conn.webhook_token || _ghlProvider.generateWebhookToken();
    const webhookUrl = `${backendUrl}/webhooks/ghl?t=${webhookToken}`;

    // Delete existing native webhook if any
    if (conn.webhook_id) {
      try {
        await _ghlProvider.deleteWebhookSubscription(accessToken, conn.webhook_id);
        console.log(`[GHL Native Webhook] Deleted old webhook id=${conn.webhook_id}`);
      } catch (e) { console.warn('[GHL Native Webhook] Could not delete old webhook:', e.message); }
    }

    console.log(`[GHL Native Webhook] Creating subscription: locationId=${locationId} url=${webhookUrl}`);
    const wh = await _ghlProvider.createWebhookSubscription(accessToken, locationId, webhookUrl);
    console.log(`[GHL Native Webhook] GHL response: ${JSON.stringify(wh)}`);
    const webhookId = wh?.id || wh?.webhookId || wh?.webhook?.id || null;
    console.log(`[GHL Native Webhook] ✓ Created webhook id=${webhookId} url=${webhookUrl}`);

    await supabase.from('calendar_integrations')
      .update({ webhook_id: webhookId, webhook_url: webhookUrl, provider_location_id: locationId })
      .eq('negocio_id', cliente_id).eq('provider', 'ghl');

    res.json({ ok: true, webhook_id: webhookId, webhook_url: webhookUrl, location_id: locationId, ghl_raw_response: wh, events: ['AppointmentCreate','AppointmentUpdate','AppointmentDelete','AppointmentRescheduled'] });
  } catch (err) {
    console.error('[GHL Native Webhook] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /ghl/debug — GHL pipeline debug (holding-only)
app.get('/ghl/debug', async (req, res) => {
  const email = req.headers['x-user-email'];
  if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
  try {
    const [connsRes, callsRes] = await Promise.all([
      supabase.from('calendar_integrations')
        .select('negocio_id, provider, provider_location_id, connected_at, status')
        .order('connected_at', { ascending: false }),
      supabase.from('calls')
        .select('id, nombre, email, estado, origen, cliente_id, fecha_llamada, provider_event_id, created_at')
        .eq('origen', 'GHL')
        .order('created_at', { ascending: false }).limit(20),
    ]);
    res.json({
      connections:     connsRes.data || [],
      recent_webhooks: _ghlWebhookLog,
      recent_calls:    callsRes.data  || [],
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /ghl/sync — manual pull sync: fetches GHL appointments and upserts into calls table
// Supports both API-key (Private Integration) and OAuth tokens stored in calendar_integrations
app.post('/ghl/sync', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });

    const cliente_id = req.query.cliente_id || req.body?.cliente_id;
    if (!cliente_id) return res.status(400).json({ error: 'cliente_id required' });
    if ((await _resolveCalendarProvider(cliente_id)) !== 'ghl') {
      return res.status(400).json({ error: `Negocio "${cliente_id}" does not use GHL` });
    }

    // Prefer token/key from DB; fall back to GHL_API_KEY env var
    let conn = await _getGhlToken(cliente_id);
    if (!conn) {
      const envKey      = process.env.GHL_API_KEY;
      const envLocation = process.env.GHL_LOCATION_ID;
      if (!envKey || !envLocation) {
        return res.status(503).json({ error: 'No GHL connection found. Configure in Holding → Integraciones or set GHL_API_KEY + GHL_LOCATION_ID in Railway.' });
      }
      conn = { access_token: envKey, provider_location_id: envLocation };
    }

    const locationId = conn.provider_location_id;
    if (!locationId) return res.status(500).json({ error: 'No location ID configured for this connection' });

    // Date range from body, or default: 30 days back → 60 days ahead
    const startTime = req.body?.startTime || new Date(Date.now() - 30 * 86400_000).toISOString();
    const endTime   = req.body?.endTime   || new Date(Date.now() + 60 * 86400_000).toISOString();

    console.log(`[GHL Sync] negocio=${cliente_id} location=${locationId} range=[${startTime}, ${endTime}]`);

    const appointments = await _ghlProvider.listAppointments(conn.access_token, locationId, startTime, endTime);
    console.log(`[GHL Sync] Fetched ${appointments.length} appointments`);

    let synced = 0, skipped = 0;
    const errors = [];

    for (const appt of appointments) {
      try {
        let contact = {};
        if (appt.contactId) {
          contact = await _ghlProvider.getContact(conn.access_token, appt.contactId)
            .catch(e => { console.warn(`[GHL Sync] Contact ${appt.contactId}: ${e.message}`); return {}; });
        }
        const apptPayload = { ...appt, appointmentId: appt.id };
        const callId = await _ghlUpsertCall(apptPayload, contact, cliente_id, 'AppointmentCreate', appt);
        if (callId) synced++; else skipped++;
      } catch (err) {
        console.error(`[GHL Sync] Appt ${appt.id} error: ${err.message}`);
        errors.push({ id: appt.id, error: err.message });
      }
    }

    console.log(`[GHL Sync] Done — synced=${synced} skipped=${skipped} errors=${errors.length}`);
    res.json({ ok: true, total: appointments.length, synced, skipped, errors });

  } catch (err) {
    console.error('[GHL Sync] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /ghl/appointments — proxy to GHL calendar events (for holding preview)
app.get('/ghl/appointments', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });

    const { cliente_id, startTime, endTime } = req.query;
    if (!cliente_id) return res.status(400).json({ error: 'cliente_id required' });

    let conn = await _getGhlToken(cliente_id);
    if (!conn && process.env.GHL_API_KEY) {
      conn = { access_token: process.env.GHL_API_KEY, provider_location_id: process.env.GHL_LOCATION_ID };
    }
    if (!conn?.access_token) return res.status(503).json({ error: 'No GHL connection' });
    if (!conn.provider_location_id) return res.status(500).json({ error: 'No location ID' });

    const start = startTime || new Date(Date.now() - 7 * 86400_000).toISOString();
    const end   = endTime   || new Date(Date.now() + 30 * 86400_000).toISOString();

    const appointments = await _ghlProvider.listAppointments(conn.access_token, conn.provider_location_id, start, end);
    res.json({ ok: true, appointments });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /holding-metrics?cliente_id=&mes=&anio=
app.get('/holding-metrics', validateAccess, async (req, res) => {
  const { cliente_id, mes, anio } = req.query;
  if (!cliente_id) return res.status(400).json({ error: 'cliente_id requerido' });
  const mesNum = Number(mes);
  const anioNum = Number(anio) || new Date().getFullYear();
  const fechaFrom = new Date(anioNum, mesNum, 1).toISOString().slice(0,10);
  const fechaTo   = new Date(anioNum, mesNum + 1, 0).toISOString().slice(0,10);
  const ESTADOS_AGENDADO = ['Agendado','Cerrado','Cerrada','Seña','Perdido Post Call','Seguimiento Post Call','Re agendado','No Show'];
  const ESTADOS_CIERRE   = ['Cerrado','Cerrada','Seña'];
  const [{ data: ingresos }, { data: leadsAll }, { data: calls }] = await Promise.all([
    supabase.from('ingresos').select('monto_usd,cash_usd').eq('cliente_id', cliente_id).gte('fecha', fechaFrom).lte('fecha', fechaTo),
    supabase.from('leads').select('estado,created_at').eq('cliente_id', cliente_id).gte('created_at', `${fechaFrom}T00:00:00`).lte('created_at', `${fechaTo}T23:59:59`),
    supabase.from('calls').select('estado,fecha_llamada,created_at').eq('cliente_id', cliente_id)
  ]);
  const facturacion    = (ingresos||[]).reduce((s,r)=>s+(+r.monto_usd||0),0);
  const cash_collected = (ingresos||[]).reduce((s,r)=>s+(+r.cash_usd||0),0);
  const leads_total    = (leadsAll||[]).length;
  const agendas        = (leadsAll||[]).filter(l=>ESTADOS_AGENDADO.includes(l.estado)).length;
  const cierres_leads  = (leadsAll||[]).filter(l=>ESTADOS_CIERRE.includes(l.estado)).length;
  const callsEnMes = (calls||[]).filter(c=>{ const d=(c.fecha_llamada||c.created_at||'').slice(0,10); return d>=fechaFrom&&d<=fechaTo; });
  const cierres = Math.max(cierres_leads, callsEnMes.filter(c=>ESTADOS_CIERRE.includes(c.estado)).length);
  res.json({ facturacion, cash_collected, leads_total, agendas, cierres });
});

// ── HOLDING EQUIPO ──────────────────────────────────────────────────────────

// GET /holding/miembros
app.get('/holding/miembros', validateAccess, async (req, res) => {
  const { data, error } = await supabase.from('holding_miembros').select('*').order('created_at', { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST /holding/miembros
app.post('/holding/miembros', validateAccess, async (req, res) => {
  const { nombre, area, rol, foto_url, salario, tareas, doc_rol, clientes_ids } = req.body;
  if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
  const now = new Date().toISOString();
  const { data, error } = await supabase.from('holding_miembros')
    .insert({ nombre, area: area||'', rol: rol||'', foto_url: foto_url||'', salario: salario||'', tareas: tareas||'', doc_rol: doc_rol||false, clientes_ids: clientes_ids||[], created_at: now, updated_at: now })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// PATCH /holding/miembros/:id
app.patch('/holding/miembros/:id', validateAccess, async (req, res) => {
  const { id } = req.params;
  const allowed = ['nombre','area','rol','foto_url','salario','tareas','doc_rol','clientes_ids'];
  const upd = {};
  allowed.forEach(k => { if (req.body[k] !== undefined) upd[k] = req.body[k]; });
  upd.updated_at = new Date().toISOString();
  const { data, error } = await supabase.from('holding_miembros').update(upd).eq('id', id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// DELETE /holding/miembros/:id
app.delete('/holding/miembros/:id', validateAccess, async (req, res) => {
  const { error } = await supabase.from('holding_miembros').delete().eq('id', req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// GET /holding/miembro-publico/:id  (public — for form page)
app.get('/holding/miembro-publico/:id', async (req, res) => {
  const { data: m, error } = await supabase.from('holding_miembros').select('id,nombre,area,rol').eq('id', req.params.id).single();
  if (error || !m) return res.status(404).json({ error: 'Miembro no encontrado' });
  // Find assigned form
  const { data: forms } = await supabase.from('holding_formularios').select('*');
  const form = (forms||[]).find(f => Array.isArray(f.miembros_ids) && f.miembros_ids.includes(m.id));
  res.json({ miembro: m, formulario: form || null });
});

// GET /holding/formularios
app.get('/holding/formularios', validateAccess, async (req, res) => {
  const { data, error } = await supabase.from('holding_formularios').select('*').order('created_at', { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST /holding/formularios
app.post('/holding/formularios', validateAccess, async (req, res) => {
  const { nombre, preguntas, miembros_ids } = req.body;
  if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
  const now = new Date().toISOString();
  const { data, error } = await supabase.from('holding_formularios')
    .insert({ nombre, preguntas: preguntas||[], miembros_ids: miembros_ids||[], created_at: now, updated_at: now })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// PATCH /holding/formularios/:id
app.patch('/holding/formularios/:id', validateAccess, async (req, res) => {
  const { nombre, preguntas, miembros_ids } = req.body;
  const upd = { updated_at: new Date().toISOString() };
  if (nombre !== undefined) upd.nombre = nombre;
  if (preguntas !== undefined) upd.preguntas = preguntas;
  if (miembros_ids !== undefined) upd.miembros_ids = miembros_ids;
  const { data, error } = await supabase.from('holding_formularios').update(upd).eq('id', req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// DELETE /holding/formularios/:id
app.delete('/holding/formularios/:id', validateAccess, async (req, res) => {
  const { error } = await supabase.from('holding_formularios').delete().eq('id', req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// GET /holding/respuestas?miembro_id=&mes=&anio=
app.get('/holding/respuestas', validateAccess, async (req, res) => {
  const { miembro_id, mes, anio } = req.query;
  let q = supabase.from('holding_respuestas').select('*');
  if (miembro_id) q = q.eq('miembro_id', miembro_id);
  if (mes !== undefined) q = q.eq('mes', Number(mes));
  if (anio !== undefined) q = q.eq('anio', Number(anio));
  const { data, error } = await q.order('semana', { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST /holding/respuestas  (public — member submits form)
app.post('/holding/respuestas', async (req, res) => {
  const { miembro_id, formulario_id, semana, mes, anio, respuestas } = req.body;
  if (!miembro_id || !semana || mes === undefined || !anio) return res.status(400).json({ error: 'Faltan campos requeridos' });
  // Upsert: one response per member per week per month/year
  const { data: existing } = await supabase.from('holding_respuestas')
    .select('id').eq('miembro_id', miembro_id).eq('semana', semana).eq('mes', mes).eq('anio', anio).single();
  let result;
  if (existing) {
    result = await supabase.from('holding_respuestas').update({ respuestas: respuestas||{}, formulario_id, updated_at: new Date().toISOString() }).eq('id', existing.id).select().single();
  } else {
    result = await supabase.from('holding_respuestas').insert({ miembro_id, formulario_id: formulario_id||null, semana, mes, anio, respuestas: respuestas||{}, created_at: new Date().toISOString() }).select().single();
  }
  if (result.error) return res.status(500).json({ error: result.error.message });
  res.json({ ok: true, data: result.data });
});

// POST /holding/reporte-ia  — global report per client
app.post('/holding/reporte-ia', validateAccess, async (req, res) => {
  // negocios: [{ cliente_id, label, facturacion, cash_collected, leads_activos }]
  const { tipo, mes, anio, negocios } = req.body;
  try {
    const [{ data: miembros }, { data: respuestas }] = await Promise.all([
      supabase.from('holding_miembros').select('*'),
      supabase.from('holding_respuestas').select('*').eq('anio', anio).eq('mes', mes)
    ]);

    const meses = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];

    // Group members by cliente_id; only include clients with assigned members
    const negociosConMiembros = (negocios||[]).filter(n => {
      return (miembros||[]).some(m => m.cliente_id === n.cliente_id);
    });

    const resumenNegocios = negociosConMiembros.map(n => {
      const miembrosNegocio = (miembros||[]).filter(m => m.cliente_id === n.cliente_id);
      const datosEquipo = miembrosNegocio.map(m => {
        const reps = (respuestas||[]).filter(r => r.miembro_id === m.id);
        return {
          nombre: m.nombre, rol: m.rol, area: m.area,
          semanas_completadas: reps.map(r => r.semana),
          respuestas: reps.map(r => ({ semana: r.semana, respuestas: r.respuestas }))
        };
      });
      return {
        negocio: n.label || n.cliente_id,
        facturacion: n.facturacion || 0,
        cash_collected: n.cash_collected || 0,
        leads_activos: n.leads_activos || 0,
        equipo: datosEquipo
      };
    });

    const prompt = `Sos un analista estratégico de una agencia de ventas. Generá un reporte ${tipo==='mensual'?'mensual':'semanal'} del holding para ${meses[mes]} ${anio}.

DATOS POR NEGOCIO:
${resumenNegocios.map(n => `
--- NEGOCIO: ${n.negocio} ---
Facturación: $${n.facturacion}
Cash Collected: $${n.cash_collected}
Leads activos en CRM: ${n.leads_activos}
Equipo asignado:
${JSON.stringify(n.equipo, null, 2)}
`).join('\n')}

Para CADA negocio generá:
1. Estado actual (facturación, cash, leads)
2. Rendimiento del equipo asignado (quién reportó y quién no)
3. Problemas identificados en base a las respuestas del equipo
4. Cuellos de botella operativos
5. Pasos de acción concretos para la próxima semana

Luego una sección final: "CONCLUSIONES GENERALES DEL HOLDING" con visión global.

Formato: markdown estructurado, claro, directo al punto y accionable.`;

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 3000, messages: [{ role: 'user', content: prompt }] })
    });
    const aiJson = await aiRes.json();
    const reporte = aiJson?.content?.[0]?.text || 'No se pudo generar el reporte.';
    res.json({ ok: true, reporte });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 🚀 SERVER
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);

  // GHL integration config check
  if (process.env.GHL_CLIENT_ID && process.env.GHL_CLIENT_SECRET) {
    const ghlIds = (process.env.GHL_NEGOCIO_IDS || '').split(',').filter(Boolean);
    console.log(`[GHL] ✓ Configured — negocios: [${ghlIds.join(', ')}]`);
    if (!process.env.BACKEND_URL) {
      console.warn('[GHL] ⚠ BACKEND_URL not set — webhook URLs will use request host. Set BACKEND_URL=https://your-railway-url.railway.app in Railway env vars for reliable webhook registration.');
    } else {
      console.log(`[GHL] ✓ BACKEND_URL=${process.env.BACKEND_URL}`);
    }
  } else {
    console.log('[GHL] Not configured (GHL_CLIENT_ID/GHL_CLIENT_SECRET missing — Calendly integration unaffected)');
  }

  _startDiscordGateway();
  _startDiscordScheduler(supabase, process.env.FRONTEND_URL);
});
