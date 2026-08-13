const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

const kafka = new Kafka({
  clientId: 'log-alerting',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'alerting-group' });

const pool = new Pool({
  user: 'loguser',
  password: 'logpass',
  host: 'localhost',
  port: 5432,
  database: 'logs_db'
});

// In-memory tracker: { serviceName: [timestamp1, timestamp2, ...] }
const recentErrors = {};

const ERROR_THRESHOLD = 5;
const WINDOW_MS = 60 * 1000; // 60 seconds

const CRITICAL_KEYWORDS = ['crash', 'timeout'];

async function fireAlert(ruleTriggered, service, details) {
  console.log(`ALERT: [${ruleTriggered}] ${service} — ${details}`);
  await pool.query(
    `INSERT INTO alerts (rule_triggered, service, details) VALUES ($1, $2, $3)`,
    [ruleTriggered, service, details]
  );
}

function checkThresholdRule(log) {
  if (log.level !== 'ERROR') return;

  const now = Date.now();
  if (!recentErrors[log.service]) recentErrors[log.service] = [];

  // Add this error's timestamp
  recentErrors[log.service].push(now);

  // Prune anything older than the 60-second window
  recentErrors[log.service] = recentErrors[log.service].filter(
    ts => now - ts <= WINDOW_MS
  );

  if (recentErrors[log.service].length >= ERROR_THRESHOLD) {
    fireAlert(
      'ERROR_THRESHOLD',
      log.service,
      `${recentErrors[log.service].length} errors in the last 60 seconds`
    );
    // Reset after firing so we don't spam an alert every single message past the threshold
    recentErrors[log.service] = [];
  }
}

function checkKeywordRule(log) {
  const lowerMessage = log.message.toLowerCase();
  const matched = CRITICAL_KEYWORDS.find(keyword => lowerMessage.includes(keyword));
  if (matched) {
    fireAlert('CRITICAL_KEYWORD', log.service, `Message contained "${matched}": ${log.message}`);
  }
}

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw-logs', fromBeginning: false });

  console.log('Alerting consumer running, group: alerting-group');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const log = JSON.parse(message.value.toString());
        checkThresholdRule(log);
        checkKeywordRule(log);
      } catch (err) {
        console.error('Error processing message:', err.message);
      }
    }
  });
}

run().catch(err => {
  console.error('Fatal alerting error:', err);
  process.exit(1);
});