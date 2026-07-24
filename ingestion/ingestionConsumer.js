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

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw-logs', fromBeginning: true });

  console.log('Ingestion consumer running, group: ingestion-group');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const log = JSON.parse(message.value.toString());

        if (!log.timestamp || !log.service || !log.level || !log.message) {
          console.error('Skipping malformed log:', log);
          return;
        }

        await pool.query(
          `INSERT INTO logs (timestamp, service, level, message, request_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [log.timestamp, log.service, log.level, log.message, log.requestId || null]
        );

        console.log('Inserted:', log.service, log.level);
      } catch (err) {
        console.error('Error processing message:', err.message);
        // Don't rethrow — one bad message shouldn't crash the whole consumer
      }
    }
  });
}

run().catch(err => {
  console.error('Fatal ingestion error:', err);
  process.exit(1);
});