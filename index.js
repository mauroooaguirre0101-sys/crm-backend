const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// ✅ Middlewares
app.use(cors());
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

// 🔥 ENDPOINT LEADS / EVENTOS
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

    // 🛑 Validación
    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    // 🧠 Limpieza nombre
    const nombreLimpio =
      nombre && !nombre.includes('{{') ? nombre : 'Sin nombre';

    // 🧠 Defaults inteligentes
    const tipoFinal = tipo || 'comentario'; // comentario o seguidor
    const origenFinal = origen || 'Inbound';
    const etiquetaFinal = etiqueta || '';

    // 🧠 Tipo de lead (para columna "tipo" del CRM)
    const tipoLead = tipoFinal === 'seguidor' ? 'Seguidor' : 'Organico';

    // 🧠 1. UPSERT (no duplica)
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
        {
          onConflict: 'instagram'
        }
      );

    if (upsertError) {
      console.error('❌ Error UPSERT:', upsertError);
      return res.status(500).json({ error: upsertError.message });
    }

    // 🧠 2. EVENTO (clave para métricas)
    const { error: eventError } = await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiquetaFinal || 'desconocido',
        tipo: tipoFinal // 👈 ACA está la magia (seguidor vs comentario)
      });

    if (eventError) {
      console.error('❌ Error evento:', eventError);
    }

    console.log('✅ Lead procesado:', {
      instagram,
      tipo: tipoFinal,
      etiqueta: etiquetaFinal
    });

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
