const http = require('http');
const path = require('path');
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { WebSocketServer, WebSocket } = require('ws');
const { Kafka } = require('kafkajs');

const app = express();
app.use(cors());

// Serve the dashboard folder at http://localhost:4000
app.use(express.static(path.join(__dirname, '..', 'dashboard')));

const pool = new Pool({
  user: 'loguser',
  password: 'logpass',
  host: 'localhost',
  port: 5432,
  database: 'logs_db'
});

// ---------- REST routes (same as Day 5) ----------
app.get('/logs', async (req, res) => {
  try {
    const { service, level, search, from, to } = req.query;
    const page = parseInt(req.query.page) || 1;
    const pageSize = 20;
    const offset = (page - 1) * pageSize;

    const conditions = [];
    const values = [];

    if (service) { values.push(service); conditions.push(`service = $${values.length}`); }
    if (level) { values.push(level); conditions.push(`level = $${values.length}`); }
    if (search) { values.push(`%${search}%`); conditions.push(`message ILIKE $${values.length}`); }
    if (from) { values.push(from); conditions.push(`timestamp >= $${values.length}`); }
    if (to) { values.push(to); conditions.push(`timestamp <= $${values.length}`); }

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

app.get('/services', async (req, res) => {
  try {
    const result = await pool.query('SELECT DISTINCT service FROM logs ORDER BY service');
    res.json(result.rows.map(r => r.service));
  } catch (err) {
    console.error('Error fetching services:', err.message);
    res.status(500).json({ error: 'Failed to fetch services' });
  }
});

// Recent alerts, so the dashboard isn't empty when first opened
app.get('/alerts', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM alerts ORDER BY id DESC LIMIT 20');
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching alerts:', err.message);
    res.status(500).json({ error: 'Failed to fetch alerts' });
  }
});

// ---------- WebSocket server ----------
// Attach it to the same HTTP server so REST and WebSocket share port 4000
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', () => {
  console.log(`Dashboard connected (${wss.clients.size} client(s))`);
});

function broadcast(type, data) {
  const payload = JSON.stringify({ type, data });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

// ---------- Kafka consumer feeding the WebSocket ----------
const kafka = new Kafka({ clientId: 'log-api', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'dashboard-group' });

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw-logs', fromBeginning: false });
  await consumer.subscribe({ topic: 'alerts', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        broadcast(topic === 'alerts' ? 'alert' : 'log', data);
      } catch (err) {
        console.error('Bad message for dashboard:', err.message);
      }
    }
  });
  console.log('Dashboard Kafka consumer running, group: dashboard-group');
}

const PORT = 4000;
server.listen(PORT, () => {
  console.log(`API + dashboard running at http://localhost:${PORT}`);
  startKafka().catch(err => console.error('Kafka consumer failed:', err));
});