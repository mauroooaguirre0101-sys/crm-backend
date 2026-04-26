const express = require('express');
const app = express();

app.use(express.json());

app.get('/', (req, res) => {
  res.send('Backend funcionando 🚀');
});

app.post('/lead', (req, res) => {
  console.log(req.body);
  res.json({ ok: true });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('Server running on port', PORT);
});
