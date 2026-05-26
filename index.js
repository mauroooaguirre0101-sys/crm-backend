const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();

// ✅ Middlewares
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '25mb' }));

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

const AI_BASE_SYSTEM = `Eres un analista estratégico de ventas de alto ticket especializado en el mercado hispanohablante. Tu rol es analizar transcripts de llamadas de ventas y actuar como un consultor experto.

Cuando analizás un transcript por primera vez, estructurás la respuesta con estas secciones usando markdown:

## Resumen ejecutivo
(2-3 oraciones sobre la llamada)

## Dolores principales detectados
(lista de bullets con los problemas que mencionó el prospecto)

## Objeciones identificadas
(lista de bullets con objeciones concretas que dijo o insinuó)

## Nivel de interés
(número del 1 al 10 con justificación de 1-2 oraciones)

## Señales de compra
(bullets con señales positivas)

## Señales de alerta
(bullets con señales de riesgo o rechazo)

## Próximos pasos recomendados
(bullets con acciones concretas priorizadas)

Para preguntas de seguimiento respondés de forma conversacional y directa, sin repetir la estructura completa a menos que se pida explícitamente. Siempre respondés en español.`;

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
  'id','estado','calificado','descalificado','tipo','origen',
  'created_at','updated_at','estado_updated_at',
  'etiqueta','etiquetas','nombre','instagram',
  'source','seguimientos','show','respondio_seguimiento_4',
].join(',');

app.get('/leads', validateAccess, async (req, res) => {
  try {
    const { after, lite, page, per_page, estado, search, period, mes, vista, sort_by, sort_dir, etiqueta_filter } = req.query;

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
    if (lite === '1' && !page) {
      const { data, error } = await supabase.from('leads')
        .select(LEADS_LITE_FIELDS)
        .eq('cliente_id', req.cliente_id)
        .order('created_at', { ascending: false })
        .limit(15000);
      if (error) { console.error('❌ GET LEADS lite:', error); return res.status(500).json({ error: error.message }); }
      return res.json(data);
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
      q = q.or(`etiqueta.eq.${ef},etiquetas.cs.{"${ef}"}`);
    }
    if (vista === 'perdidos')  q = q.or('estado.eq.Perdido,and(seguimientos.gte.4,respondio_seguimiento_4.eq.NO)');
    if (vista === 'activos')   q = q.neq('estado', 'Perdido');

    const now = new Date();
    if (mes !== undefined && mes !== '') {
      const m  = parseInt(mes, 10);
      const yr = now.getFullYear();
      q = q.gte('created_at', new Date(yr, m, 1).toISOString())
           .lte('created_at', new Date(yr, m + 1, 0, 23, 59, 59).toISOString());
    } else if (period) {
      const offsets = { dia: 0, semana: 7, mes: 30, año: 365 };
      const days = offsets[period];
      if (days !== undefined) {
        const from = new Date(now);
        if (days === 0) from.setHours(0, 0, 0, 0);
        else from.setDate(now.getDate() - days);
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
      tipo:          ['Ads','Organico','Outbound','Seguidor'].includes(tipo) ? tipo : 'Organico',
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
    const { nombre, instagram, whatsapp, info_previa, origen, fecha_llamada } = req.body;

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
        ...(fecha_llamada ? { fecha_llamada } : {})
      });

    if (error) {
      console.error('❌ PRECALL:', error);
      return res.status(500).json({ error: error.message });
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
      reporte,
      info_previa,
      reporte_ghl,
      fecha_llamada
    } = req.body;

    motivo_no_cierre = motivo_no_cierre || '';
    link_llamada = link_llamada || '';
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
    if (reporte          !== undefined) patch.reporte            = reporte;
    if (info_previa      !== undefined) patch.info_previa        = info_previa;
    if (reporte_ghl      !== undefined) patch.reporte_ghl        = reporte_ghl;
    if ('fecha_llamada' in req.body)    patch.fecha_llamada      = fecha_llamada || null;

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
    }

    if (nuevoEstadoLead) {
      await supabase
        .from('leads')
        .update({ estado: nuevoEstadoLead })
        .eq('instagram', callData.instagram)
        .eq('cliente_id', req.cliente_id);
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
  try {
    const { nombre, instagram, mensaje, origen, tipo, etiqueta } = req.body;

    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    const nombreLimpio =
      nombre && !nombre.includes('{{') ? nombre : 'Sin nombre';

    const tipoFinal = tipo || 'comentario';
    const ALLOWED_ORIGEN = ['Inbound', 'Outbound'];
    const origenFinal = ALLOWED_ORIGEN.includes(origen) ? origen : 'Inbound';
    const etiquetaFinal = etiqueta || '';
    const tipoLead = tipoFinal === 'seguidor' ? 'Seguidor' : 'Organico';
    const now = new Date().toISOString();

    // Check if lead already exists for this client
    const { data: existingArr } = await supabase
      .from('leads')
      .select('id, etiquetas, etiqueta')
      .eq('instagram', instagram)
      .eq('cliente_id', req.cliente_id)
      .limit(1);

    const existing = existingArr?.[0] || null;

    if (existing) {
      // Append new etiqueta to array — never overwrite
      const prev = Array.isArray(existing.etiquetas) && existing.etiquetas.length
        ? existing.etiquetas
        : (existing.etiqueta ? [existing.etiqueta] : []);
      const newEtiquetas = etiquetaFinal ? [...prev, etiquetaFinal] : prev;

      const { error: updateError } = await supabase
        .from('leads')
        .update({ etiquetas: newEtiquetas, ultima_accion: mensaje || '', updated_at: now })
        .eq('id', existing.id)
        .eq('cliente_id', req.cliente_id);

      if (updateError) {
        console.error('❌ UPDATE LEAD (webhook):', updateError);
        return res.status(500).json({ error: updateError.message });
      }
    } else {
      // New lead
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
    }

    await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiquetaFinal || 'desconocido',
        tipo: tipoFinal,
        cliente_id: req.cliente_id
      });

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
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

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ SERVER:', err);
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
      curStart  = new Date(todayUTC); curStart.setUTCDate(todayUTC.getUTCDate() - 30);
      prevEnd   = new Date(curStart);
      prevStart = new Date(curStart); prevStart.setUTCDate(prevStart.getUTCDate() - 30);
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
      const shows          = c.filter(x => x.estado !== 'No asistió' && x.estado !== 'Re agenda' && x.estado !== 'Pendiente').length;
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
    res.json(data || []);
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
    if (req.body.rules  !== undefined) updates.rules  = req.body.rules;
    const { data, error } = await supabase.from('equipo_members')
      .update(updates).eq('id', req.params.id).eq('cliente_id', req.cliente_id)
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
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
            implementacion, porque_no, extra } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'Falta cliente_id' });
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
        const fields = { situacion, objetivos, logros, problemas, ayuda: ayuda || [], implementacion, porque_no: porque_no || '', extra: extra || '', estado: estado || '', semana: semana || '', locked: true, editable_until: null };
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
app.get('/holding/tareas', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const negocio_id = req.query.negocio_id;
    let q = supabase.from('tareas_holding').select('*').order('orden', { ascending: true });
    if (negocio_id) q = q.eq('negocio_id', negocio_id);
    const { data, error } = await q;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/holding/tareas', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { data, error } = await supabase.from('tareas_holding').insert({ ...req.body, created_by: email }).select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.patch('/holding/tareas/:id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { data, error } = await supabase.from('tareas_holding').update(req.body).eq('id', req.params.id).select('*').single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/holding/tareas/:id', async (req, res) => {
  try {
    const email = req.headers['x-user-email'];
    if (!(await holdingAccess(email))) return res.status(403).json({ error: 'Sin acceso a holding' });
    const { error } = await supabase.from('tareas_holding').delete().eq('id', req.params.id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
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

app.get('/form-template', async (req, res) => {
  try {
    const { cliente_id, tipo } = req.query;
    if (!cliente_id || !tipo) return res.status(400).json({ error: 'Faltan parámetros' });
    const { data } = await supabase.from('form_templates').select('questions').eq('cliente_id', cliente_id).eq('tipo', tipo).maybeSingle();
    const saved = data?.questions;
    res.json({ questions: (Array.isArray(saved) && saved.length > 0) ? saved : (DEFAULT_QUESTIONS[tipo] || []) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/form-template', validateAccess, async (req, res) => {
  try {
    const { tipo, questions } = req.body;
    if (!tipo || !Array.isArray(questions)) return res.status(400).json({ error: 'Datos inválidos' });
    const { data, error } = await supabase.from('form_templates')
      .upsert({ cliente_id: req.cliente_id, tipo, questions, updated_at: new Date().toISOString() }, { onConflict: 'cliente_id,tipo' })
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
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }]
    });

    const assistantResponse = completion.content[0].text;
    const messages = [
      { role: 'user', content: userMessage },
      { role: 'assistant', content: assistantResponse }
    ];

    const insertData = { cliente_id: req.cliente_id, transcript: transcript.trim(), messages };
    if (call_id) insertData.call_id = call_id;

    const { data: saved, error: saveErr } = await supabase
      .from('call_analyses')
      .insert(insertData)
      .select('id, created_at')
      .single();

    if (saveErr) {
      console.error('❌ AI SAVE:', saveErr);
      return res.json({ id: null, response: assistantResponse, messages });
    }

    res.json({ id: saved.id, response: assistantResponse, messages, created_at: saved.created_at });
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
      .select('id, call_id, created_at, updated_at, transcript')
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

// ============================================================
// 📊 WEEKLY REPORTS — Constantes (espejo de script.js frontend)
// ============================================================

const REPORT_FUNNEL_FASES = [
  { label: 'Primer Contacto',  estados: ['Primer contacto'] },
  { label: 'Descubrimiento',   estados: ['Descubrimiento (Problemas-Objetivos)'] },
  { label: 'Nutrición',        estados: ['Recurso de nutrición'] },
  { label: 'Agendamiento',     estados: ['PITCH VSL CHAT', 'VSL CHAT', 'Proponer Call', 'Calendly Enviado'] },
  { label: 'Cierre',           estados: ['Agendado'] },
  { label: 'Cerrados',         estados: ['Cerrada', 'Seña'] },
];
const REPORT_ESTADO_CERRADO = new Set(['Cerrada', 'Seña']);
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
    const SEG_LABELS = new Set(['seguidor nuevo', 'seguir nuevo']);
    const _isSegNuevo = (lead) => {
      const ets = Array.isArray(lead.etiquetas) && lead.etiquetas.length
        ? lead.etiquetas : lead.etiqueta ? [lead.etiqueta] : [];
      return ets.some(e => SEG_LABELS.has((e || '').toLowerCase().trim()));
    };
    const cerradosNow    = leadsNow.filter(l => REPORT_ESTADO_CERRADO.has(l.estado));
    const agendasCount   = leadsNow.filter(l => l.estado === 'Agendado').length;
    const facturacion    = ingCur.filter(i => i.concepto === 'Venta Nueva').reduce((s, i) => s + (Number(i.usd) || 0), 0);
    const cashCollected  = cliCur.reduce((s, c) => s + (Number(c.cash_collected) || 0), 0);
    const egresoTotal    = egresosCur.reduce((s, e) => s + (Number(e.usd) || 0), 0);
    const showsCount     = callsCur.filter(c => !['No asistió', 'Re agenda', 'Pendiente'].includes(c.estado)).length;
    const aov            = cerradosNow.length > 0 ? Math.round(facturacion / cerradosNow.length) : 0;
    const seguidoresNow  = leadsNow.filter(_isSegNuevo).length;
    const orgLeadsNow    = leadsNow.length - seguidoresNow;
    const seguidoresPrev = leadsPrev.filter(_isSegNuevo).length;
    const orgLeadsPrev   = leadsPrev.length - seguidoresPrev;

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
        seguidores_nuevos: seguidoresPrev,
        leads:             orgLeadsPrev,
        cerrados:          cerradosPrev.length,
        facturacion:       facturacionPrev,
        cash_collected:    cashCollectedPrev,
        calls:             callsPrev.length,
      },
      delta_seguidores:     _reportFmtDelta(seguidoresNow,  seguidoresPrev),
      delta_leads:          _reportFmtDelta(orgLeadsNow,    orgLeadsPrev),
      delta_cerrados:       _reportFmtDelta(cerradosNow.length,    cerradosPrev.length),
      delta_facturacion:    _reportFmtDelta(facturacion,           facturacionPrev),
      delta_cash_collected: _reportFmtDelta(cashCollected,         cashCollectedPrev),
      delta_calls:          _reportFmtDelta(callsCur.length,       callsPrev.length),
    };

    // ── Objeto metricas final ──
    const metricas = {
      ventas: {
        seguidores_nuevos: seguidoresNow,
        leads:             orgLeadsNow,
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

// 🚀 SERVER
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
  _startDiscordGateway();
  _startDiscordScheduler(supabase, process.env.FRONTEND_URL);
});
