const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

const kafka = new Kafka({
  clientId: 'log-ingestion',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'ingestion-group' });

const pool = new Pool({
  user: 'loguser',
  password: 'logpass',
  host: 'localhost',
  port: 5432,
  database: 'logs_db'
});

// Without this, a dropped idle DB connection can crash the whole process
pool.on('error', err => console.error('Idle DB client error:', err.message));

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw-logs', fromBeginning: true });

  console.log(`[pid ${process.pid}] Ingestion consumer running, group: ingestion-group`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      // 1. Bad data: skip it. Retrying a malformed message would never help.
      let log;
      try {
        log = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('Skipping unparseable message');
        return;
      }
      if (!log.timestamp || !log.service || !log.level || !log.message) {
        console.error('Skipping malformed log:', log);
        return;
      }

      // 2. Infrastructure failure (DB down): do NOT catch it.
      // The error makes kafkajs retry this message instead of marking it done,
      // so nothing is lost while Postgres is unavailable.
      await pool.query(
        `INSERT INTO logs (timestamp, service, level, message, request_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (request_id) DO NOTHING`,
        [log.timestamp, log.service, log.level, log.message, log.requestId || null]
      );

      console.log(`[pid ${process.pid}] partition ${partition} inserted: ${log.service} ${log.level}`);
    }
  });
}

// Clean shutdown: lets Kafka hand this consumer's partitions to others immediately
async function shutdown() {
  console.log('Shutting down...');
  try {
    await consumer.disconnect();
    await pool.end();
  } finally {
    process.exit(0);
  }
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run().catch(err => {
  console.error('Fatal ingestion error:', err);
  process.exit(1);
});