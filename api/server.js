const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());

const pool = new Pool({
  user: 'loguser',
  password: 'logpass',
  host: 'localhost',
  port: 5432,
  database: 'logs_db'
});

// GET /logs?service=payment-service&level=ERROR&search=timeout&page=1
app.get('/logs', async (req, res) => {
  try {
    const { service, level, search, from, to } = req.query;
    const page = parseInt(req.query.page) || 1;
    const pageSize = 20;
    const offset = (page - 1) * pageSize;

    const conditions = [];
    const values = [];

    if (service) {
      values.push(service);
      conditions.push(`service = $${values.length}`);
    }
    if (level) {
      values.push(level);
      conditions.push(`level = $${values.length}`);
    }
    if (search) {
      values.push(`%${search}%`);
      conditions.push(`message ILIKE $${values.length}`);
    }
    if (from) {
      values.push(from);
      conditions.push(`timestamp >= $${values.length}`);
    }
    if (to) {
      values.push(to);
      conditions.push(`timestamp <= $${values.length}`);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    values.push(pageSize, offset);
    const query = `
      SELECT * FROM logs
      ${whereClause}
      ORDER BY timestamp DESC
      LIMIT $${values.length - 1} OFFSET $${values.length}
    `;

    const result = await pool.query(query, values);
    res.json({ page, pageSize, count: result.rows.length, logs: result.rows });
  } catch (err) {
    console.error('Error fetching logs:', err.message);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// GET /services — distinct service names, useful for building filter dropdowns later
app.get('/services', async (req, res) => {
  try {
    const result = await pool.query('SELECT DISTINCT service FROM logs ORDER BY service');
    res.json(result.rows.map(r => r.service));
  } catch (err) {
    console.error('Error fetching services:', err.message);
    res.status(500).json({ error: 'Failed to fetch services' });
  }
});

const PORT = 4000;
app.listen(PORT, () => {
  console.log(`API running at http://localhost:${PORT}`);
});