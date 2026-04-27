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

// 🟢 Test
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});


// ===============================
// 🔥 GET CALLS
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
// 🔥 PRE-CALL (SETTER)
// ===============================
app.post('/call/precall', async (req, res) => {
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

    // 🔢 calcular número de llamada
    const { data: existingCalls } = await supabase
      .from('calls')
      .select('id')
      .eq('instagram', instagram);

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
        responde: false
      });

    if (error) {
      console.error('❌ Error PRECALL:', error);
      return res.status(500).json({ error: error.message });
    }

    console.log('✅ Pre-call creada:', instagram);

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


// ===============================
// 🔥 UPDATE CALL (CLOSER)
// ===============================
app.patch('/call/:id', async (req, res) => {
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

    // 🔧 limpieza
    motivo_no_cierre = motivo_no_cierre || '';
    link_llamada = link_llamada || '';
    reporte = reporte || '';

    seguimientos = parseInt(seguimientos) || 0;

    if (typeof responde === 'string') {
      responde = responde.toLowerCase() === 'si';
    } else {
      responde = Boolean(responde);
    }

    const { data: callData, error: fetchError } = await supabase
      .from('calls')
      .select('*')
      .eq('id', id)
      .single();

    if (fetchError) {
      return res.status(500).json({ error: fetchError.message });
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
      .eq('id', id);

    if (error) {
      console.error('❌ Error UPDATE CALL:', error);
      return res.status(500).json({ error: error.message });
    }

    // 🔁 SYNC LEAD
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

    if (nuevoEstadoLead && callData?.instagram) {
      const { error: updateError } = await supabase
        .from('leads')
        .update({ estado: nuevoEstadoLead })
        .eq('instagram', callData.instagram);

      if (updateError) {
        console.error('⚠️ Error actualizando lead:', updateError);
      }
    }

    console.log('✅ Call actualizada:', id);

    res.json({ ok: true });

  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});


// ===============================
// 🔥 DELETE CALL
// ===============================
app.delete('/call/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('calls')
      .delete()
      .eq('id', id);

    if (error) {
      return res.status(500).json({ error: error.message });
    }

    res.json({ ok: true });

  } catch (err) {
    res.status(500).json({ error: 'Error servidor' });
  }
});


// ===============================
// 🔥 ENDPOINT LEADS (SIN CAMBIOS)
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


// 🚀 Puerto
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
