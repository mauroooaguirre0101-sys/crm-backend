const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// ✅ Middlewares
app.use(cors({ origin: '*' }));
app.use(express.json());

// 🔑 Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

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
app.get('/leads', validateAccess, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('leads')
      .select('*')
      .eq('cliente_id', req.cliente_id)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('❌ GET LEADS:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

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
    const origenFinal = origen || 'Inbound';
    const etiquetaFinal = etiqueta || '';

    const tipoLead = tipoFinal === 'seguidor' ? 'Seguidor' : 'Organico';

    const { error: upsertError } = await supabase
      .from('leads')
      .upsert(
        {
          nombre: nombreLimpio,
          instagram,
          ultima_accion: mensaje || '',
          origen: origenFinal,
          tipo: tipoLead,
          estado: 'Primer Contacto',
          etiqueta: etiquetaFinal,
          source: 'manychat',
          cliente_id: req.cliente_id
        },
        { onConflict: 'instagram' }
      );

    if (upsertError) {
      console.error('❌ UPSERT:', upsertError);
      return res.status(500).json({ error: upsertError.message });
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
      nombre,
      instagram,
      inicio,
      fin,
      tipo_pago,
      cash_collected,
      comprobante,
      estado
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
        estado: estado || 'Al día'
      }])
      .select()
      .single();

    if (error) {
      console.error('❌ CREATE CLIENTE:', error);
      return res.status(500).json({ error: error.message });
    }

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


// 🚀 SERVER
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
