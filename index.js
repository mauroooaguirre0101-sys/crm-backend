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
  process.env.SUPABASE_KEY // service_role en Railway (correcto)
);

// 🟢 Test
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});

// 🔥 ENDPOINT LEADS
app.post('/lead', async (req, res) => {
  try {
    const { nombre, instagram, mensaje, origen } = req.body;

    // 🛑 Validación básica
    if (!instagram) {
      return res.status(400).json({ error: 'Falta instagram' });
    }

    // 🧠 Limpieza de nombre (evita placeholders rotos)
    const nombreLimpio =
      nombre && !nombre.includes('{{') ? nombre : 'Sin nombre';

    // 🧠 Etiqueta (Reel X)
    const etiqueta = origen || '';

    // 🧠 1. UPSERT (no duplica por instagram)
    const { error: upsertError } = await supabase
      .from('leads')
      .upsert(
        {
          nombre: nombreLimpio,
          instagram,
          ultima_accion: mensaje || '',

          // 🔥 Estructura correcta
          origen: 'Inbound',
          tipo: 'Organico',
          estado: 'Nuevo',
          etiqueta: etiqueta,

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

    // 🧠 2. EVENTO (tracking para métricas)
    const { error: eventError } = await supabase
      .from('lead_events')
      .insert({
        instagram,
        origen: etiqueta || 'desconocido',
        tipo: 'comentario'
      });

    if (eventError) {
      console.error('❌ Error evento:', eventError);
      // no frenamos el flujo
    }

    console.log('✅ Lead procesado:', {
      instagram,
      etiqueta
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
