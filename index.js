const express = require('express');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// 🔑 Conexión a Supabase usando variables de Railway
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

app.use(express.json());

// 🟢 Ruta de prueba
app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});

// 🔥 Endpoint para guardar leads
app.post('/lead', async (req, res) => {
  try {
    const { nombre, instagram, mensaje, cliente_id } = req.body;

    // ✅ Validación correcta
    if (!nombre || !instagram || !cliente_id) {
      return res.status(400).json({ error: 'Faltan datos obligatorios' });
    }

    const { data, error } = await supabase
      .from('leads')
      .insert([
        {
          nombre,
          instagram,
          notas: mensaje || '',
          origen: 'Instagram',
          tipo: 'Organico',
          estado: 'Nuevo',
          source: 'manychat',
          cliente_id: cliente_id // 🔥 ahora es dinámico
        }
      ]);

    if (error) {
      console.error('❌ Error Supabase:', error);
      return res.status(500).json({ error: error.message });
    }

    console.log('✅ Lead guardado:', data);

    res.json({ ok: true, data });

  } catch (err) {
    console.error('🔥 Error servidor:', err);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// 🚀 Puerto (Railway lo define automáticamente)
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
