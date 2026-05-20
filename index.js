const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');

const app = express();

// ✅ Middlewares
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '10mb' }));

// 🔑 Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
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
    const { after, lite, page, per_page, estado, search, period, mes, vista } = req.query;

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

    let q = supabase.from('leads')
      .select('*', { count: 'exact' })
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false })
      .range(offset, offset + perPage - 1);

    if (estado)               q = q.eq('estado', estado);
    if (search && search.trim()) q = q.or(`nombre.ilike.%${search.trim()}%,instagram.ilike.%${search.trim()}%`);
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
    const { nombre, instagram, whatsapp, info_previa } = req.body;

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
        estado: 'Pendiente',
        numero_llamada,
        seguimientos: 0,
        responde: false,
        cliente_id: req.cliente_id
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
      reporte
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

    const { error } = await supabase
      .from('calls')
      .update({
        estado,
        motivo_no_cierre,
        seguimientos,
        responde,
        link_llamada,
        reporte
      })
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
      const shows          = c.filter(x => x.estado !== 'No asistió' && x.estado !== 'Re agenda').length;
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

// Público: alumnos envían su reporte sin necesitar auth
app.post('/reportes', async (req, res) => {
  try {
    let { cliente_id, alumno_id, nombre, apellido, instagram, semana, estado,
            situacion, objetivos, logros, problemas, ayuda,
            implementacion, porque_no, extra } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'Falta cliente_id' });
    // Verificar que el cliente_id existe
    const { data: check } = await supabase.from('user_clientes')
      .select('cliente_id').eq('cliente_id', cliente_id).limit(1);
    if (!check || check.length === 0) return res.status(400).json({ error: 'Cliente inválido' });

    // Auto-asignar alumno por instagram si no viene alumno_id
    const igClean = instagram ? instagram.toLowerCase().replace(/^@+/, '').trim() : '';
    if (!alumno_id && igClean) {
      // 1. Busca directo en alumnos por instagram
      const { data: matchDirect } = await supabase.from('alumnos')
        .select('id').eq('cliente_id', cliente_id).eq('instagram', igClean).maybeSingle();
      if (matchDirect) {
        alumno_id = matchDirect.id;
      } else {
        // 2. Fallback: busca por clientes.instagram → alumnos.source_id
        const { data: matchCliente } = await supabase.from('clientes')
          .select('id').eq('cliente_id', cliente_id).eq('instagram', igClean).maybeSingle();
        if (matchCliente) {
          const { data: matchAlumno } = await supabase.from('alumnos')
            .select('id').eq('cliente_id', cliente_id).eq('source_id', matchCliente.id).maybeSingle();
          if (matchAlumno) alumno_id = matchAlumno.id;
        }
      }
    }

    const { data, error } = await supabase.from('reportes_semanales').insert([{
      cliente_id,
      alumno_id: alumno_id || null,
      nombre: nombre || '',
      apellido: apellido || '',
      instagram: instagram ? instagram.toLowerCase().replace(/^@/, '') : '',
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
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
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

      // Cierres: llamadas con estado Cierre
      let cq2 = supabase.from('calls').select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Cierre');
      if (from) cq2 = cq2.gte('created_at', from + 'T00:00:00.000Z');
      if (to)   cq2 = cq2.lte('created_at', to   + 'T23:59:59.999Z');
      const { data: callPeriod } = await cq2;

      // Datos anuales para gráfico de evolución mensual
      const { data: ingYear } = await supabase.from('ingresos')
        .select('usd,fecha').eq('cliente_id', cid)
        .gte('fecha', `${year}-01-01`)
        .lte('fecha', `${year}-12-31`);

      const { data: cliYear } = await supabase.from('clientes')
        .select('cash_collected,created_at').eq('cliente_id', cid)
        .gte('created_at', `${year}-01-01T00:00:00.000Z`)
        .lte('created_at', `${year}-12-31T23:59:59.999Z`);

      const { data: callYear } = await supabase.from('calls')
        .select('estado,created_at').eq('cliente_id', cid).eq('estado', 'Cierre')
        .gte('created_at', `${year}-01-01T00:00:00.000Z`)
        .lte('created_at', `${year}-12-31T23:59:59.999Z`);

      const monthly = Array.from({ length: 12 }, (_, i) => {
        const m = String(i + 1).padStart(2, '0');
        const mIng  = (ingYear  || []).filter(x => (x.fecha || '').slice(5, 7) === m);
        const mCli  = (cliYear  || []).filter(x => (x.created_at || '').slice(5, 7) === m);
        const mCall = (callYear || []).filter(x => (x.created_at || '').slice(5, 7) === m);
        return {
          facturacion:   mIng.reduce((s, x) => s + (parseFloat(x.usd) || 0), 0),
          cash_collected:mCli.reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0),
          closes:        mCall.length
        };
      });

      return {
        cliente_id:    cid,
        facturacion:   (ingPeriod  || []).reduce((s, x) => s + (parseFloat(x.usd)           || 0), 0),
        cash_collected:(cliPeriod  || []).reduce((s, x) => s + (parseFloat(x.cash_collected) || 0), 0),
        closes:        (callPeriod || []).length,
        monthly
      };
    }));

    res.json(results);
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
app.delete('/sops/:id', validateAccess, async (req, res) => {
  try {
    const { error } = await supabase.from('sops').delete().eq('id', req.params.id).eq('cliente_id', req.cliente_id);
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

// 🚀 SERVER
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
