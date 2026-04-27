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

// 🔥 ENDPOINT LEADS (CORREGIDO)
app.post('/lead', async (req, res) => {
  try {
    const { nombre, instagram, mensaje, origen } = req.body;

    // 🛑 Validación
    if (!nombre || !instagram) {
      return res.status(400).json({ error: 'Faltan datos obligatorios' });
    }

    // 🧠 1. UPSERT (NO DUPLICA)
    const { error: upsertError } = await supabase
      .from('leads')
      .upsert(
        {
          nombre,
          instagram,
          ultima_accion: mensaje || '',
          origen: 'Inbound',
          tipo: 'Organico',
          estado: 'Nuevo',
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

    // 🧠 2. GUARDAR EVENTO (CLAVE PARA MÉTRICAS)
    const { error: eventError } = await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: origen || 'desconocido',
        tipo: 'comentario'
      });

    if (eventError) {
      console.error('❌ Error evento:', eventError);
      // NO frenamos el flujo por esto
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
