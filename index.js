const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// ✅ Middlewares
app.use(cors({
  origin: '*'
}));
app.use(express.json());

// 🔑 Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// 🟢 Test
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});


// ===============================
// 🔥 GET CALLS (FALTABA ESTO)
// ===============================
app.get('/calls', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('calls')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) {
      console.error('❌ Error GET CALLS:', error);
      return res.status(500).json({ error: error.message });
    }

    res.json(data);

  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


// ===============================
// 🔥 ENDPOINT LEADS
// ===============================
app.post('/lead', async (req, res) => {
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
          cliente_id: 'cliente_1'
        },
        { onConflict: 'instagram' }
      );

    if (upsertError) {
      console.error('❌ Error UPSERT:', upsertError);
      return res.status(500).json({ error: upsertError.message });
    }

    const { error: eventError } = await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiquetaFinal || 'desconocido',
        tipo: tipoFinal
      });

    if (eventError) {
      console.error('❌ Error evento:', eventError);
    }

    console.log('✅ Lead procesado:', instagram);

    res.json({ ok: true });
  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


// ===============================
// 🔥 CREATE CALL
// ===============================
app.post('/call', async (req, res) => {
  try {
    console.log('📥 DATA RECIBIDA:', req.body);

    let {
      nombre,
      instagram,
      whatsapp,
      estado,
      seguimientos,
      responde,
      link_llamada,
      motivo_no_cierre
    } = req.body;

    if (!instagram || !estado) {
      return res.status(400).json({ error: 'Faltan datos obligatorios' });
    }

    nombre = nombre || 'Sin nombre';
    whatsapp = whatsapp || '';
    link_llamada = link_llamada || '';
    motivo_no_cierre = motivo_no_cierre || '';

    // 🔥 FIX TIPOS
    seguimientos = parseInt(seguimientos) || 0;

    if (typeof responde === 'string') {
      responde = responde.toLowerCase() === 'si';
    } else {
      responde = Boolean(responde);
    }

    console.log('📦 DATA LIMPIA:', {
      nombre,
      instagram,
      whatsapp,
      estado,
      seguimientos,
      responde,
      link_llamada,
      motivo_no_cierre
    });

    const { error: callError } = await supabase
      .from('calls')
      .insert({
        nombre,
        instagram,
        whatsapp,
        estado,
        seguimientos,
        responde,
        link_llamada,
        motivo_no_cierre
      });

    if (callError) {
      console.error('❌ Error CALL:', callError);
      return res.status(500).json({ error: callError.message });
    }

    // 🔁 SYNC LEADS
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
      const { error: updateError } = await supabase
        .from('leads')
        .update({ estado: nuevoEstadoLead })
        .eq('instagram', instagram);

      if (updateError) {
        console.error('⚠️ Error actualizando lead:', updateError);
      }
    }

    console.log('✅ Call creada:', instagram);

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


// 🚀 Puerto
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
