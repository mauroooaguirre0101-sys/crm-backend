const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// ✅ Middlewares
app.use(cors());
app.use(express.json());

// 🔑 Conexión a Supabase usando variables de Railway
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// 🟢 Ruta de prueba
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});

// 🔥 Endpoint para guardar leads
app.post('/lead', async (req, res) => {
  try {
    const { nombre, instagram, mensaje } = req.body;

    // Validación básica
    if (!nombre || !instagram) {
      return res.status(400).json({ error: 'Faltan datos obligatorios' });
    }

    const { data, error } = await supabase
      .from('leads')
      .insert([
        {
          nombre,
          instagram,
          ultima_accion: mensaje || '',
          origen: 'Inbound',
          tipo: 'Organico',
          estado: 'Nuevo',
          source: 'manychat',
          cliente_id: 'cliente_1' // luego lo hacemos dinámico
        }
      ]);

    if (error) {
      console.error('❌ Error Supabase:', error);
      return res.status(500).json({ error: error.message });
    }

    console.log('✅ Lead guardado:', data);

    res.json({ ok: true });
  } catch (err) {
    console.error('❌ Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// 🚀 Puerto (Railway lo define automáticamente)
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
