const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

app.use(cors({ origin: '*' }));
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// ===============================
// 🧠 MIDDLEWARE VALIDACIÓN CLIENTE
// ===============================
async function validateAccess(req, res, next) {
  const cliente_id = req.headers['x-cliente-id'];
  const user_email = req.headers['x-user-email'];

  if (!cliente_id || !user_email) {
    return res.status(400).json({ error: 'Faltan headers' });
  }

  const { data, error } = await supabase
    .from('user_clientes')
    .select('*')
    .eq('user_email', user_email)
    .eq('cliente_id', cliente_id)
    .single();

  if (error || !data) {
    return res.status(403).json({ error: 'Sin acceso a este cliente' });
  }

  req.cliente_id = cliente_id;
  req.user_email = user_email;
  req.user_role = data.role;

  next();
}

// ===============================
// 🟢 TEST
// ===============================
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});

// ===============================
// 🔥 USER CLIENTES
// ===============================
app.get('/user-clientes', async (req, res) => {
  try {
    const { email } = req.query;

    const { data, error } = await supabase
      .from('user_clientes')
      .select('cliente_id, role')
      .eq('user_email', email);

    if (error) {
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

  } catch (err) {
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
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

// ===============================
// 🔥 PRE-CALL (SETTER)
// ===============================
app.post('/call/precall', validateAccess, async (req, res) => {
  try {
    const {
      nombre,
      instagram,
      whatsapp,
      info_previa
    } = req.body;

    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    const { data: existingCalls } = await supabase
      .from('calls')
      .select('id')
      .eq('instagram', instagram)
      .eq('cliente_id', req.cliente_id);

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
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

// ===============================
// 🔥 UPDATE CALL (CLOSER)
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
      .single();

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
      return res.status(500).json({ error: error.message });
    }

    let nuevoEstadoLead = null;

    switch (estado) {
      case 'Cierre':
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

    if (nuevoEstadoLead && callData.instagram) {
      await supabase
        .from('leads')
        .update({ estado: nuevoEstadoLead })
        .eq('instagram', callData.instagram)
        .eq('cliente_id', req.cliente_id);
    }

    res.json({ ok: true });

  } catch (err) {
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
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

// ===============================
// 🔥 LEADS
// ===============================
app.post('/lead', validateAccess, async (req, res) => {
  try {
    const {
      nombre,
      instagram,
      mensaje,
      origen,
      tipo,
      etiqueta
    } = req.body;

    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    const tipoLead = tipo === 'seguidor' ? 'Seguidor' : 'Organico';

    const { error } = await supabase
      .from('leads')
      .upsert({
        nombre: nombre || 'Sin nombre',
        instagram,
        ultima_accion: mensaje || '',
        origen: origen || 'Inbound',
        tipo: tipoLead,
        estado: 'Primer Contacto',
        etiqueta: etiqueta || '',
        source: 'manychat',
        cliente_id: req.cliente_id
      }, { onConflict: 'instagram' });

    if (error) {
      return res.status(500).json({ error: error.message });
    }

    await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiqueta || 'desconocido',
        tipo: tipo || 'comentario',
        cliente_id: req.cliente_id
      });

    res.json({ ok: true });

  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});

// ===============================
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
