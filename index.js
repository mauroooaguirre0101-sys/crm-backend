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

// 🟢 Test
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});


// ===============================
// 🔥 GET LEADS (FIX CLAVE)
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
// 🔥 PRE-CALL (SETTER)
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


// 🚀 SERVER
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
