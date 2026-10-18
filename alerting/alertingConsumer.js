require('dotenv').config();
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

// --- Notification settings (new) ---
const WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL;

// Tracks the last time we notified for a given service+rule combo
const lastNotified = {};
const COOLDOWN_MS = 5 * 60 * 1000; // 5 minutes

async function sendNotification(ruleTriggered, service, details) {
  if (!WEBHOOK_URL) {
    console.error('DISCORD_WEBHOOK_URL is not set. Check your .env file.');
    return;
  }

  const key = `${service}:${ruleTriggered}`;
  const now = Date.now();

  if (lastNotified[key] && now - lastNotified[key] < COOLDOWN_MS) {
    console.log(`Skipping notification for ${key} — still in cooldown`);
    return;
  }

  lastNotified[key] = now;

  try {
    const response = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        content: `🚨 **${ruleTriggered}** — \`${service}\`\n${details}`
      })
    });

    if (!response.ok) {
      console.error('Webhook returned an error status:', response.status);
      return;
    }
    console.log('Notification sent to Discord');
  } catch (err) {
    console.error('Failed to send notification:', err.message);
  }
}

async function fireAlert(ruleTriggered, service, details) {
  console.log(`ALERT: [${ruleTriggered}] ${service} — ${details}`);

  // 1. Always save the alert to Postgres (history is never throttled)
  try {
    await pool.query(
      `INSERT INTO alerts (rule_triggered, service, details) VALUES ($1, $2, $3)`,
      [ruleTriggered, service, details]
    );
  } catch (err) {
    console.error('Failed to save alert to Postgres:', err.message);
  }

  // 2. Then notify (subject to cooldown)
  await sendNotification(ruleTriggered, service, details);
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