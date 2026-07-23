const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'log-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const services = ['auth-service', 'payment-service', 'api-gateway', 'inventory-service'];

const messagesByLevel = {
  INFO: ['request handled', 'health check passed', 'cache refreshed', 'user logged in'],
  WARN: ['slow response time', 'retrying connection', 'cache miss'],
  ERROR: ['card declined', 'database timeout', 'null pointer exception', 'connection refused']
};

/** 
 * Return a weighted random log level.
 * Mostly returns 'INFO', sometimes 'WARN', rarely 'ERROR'. 
 * @returns {'INFO'|'WARN'|'ERROR'} selected log level
 */
function randomLevel() {
  const roll = Math.random();
  if (roll < 0.7) return 'INFO';
  if (roll < 0.9) return 'WARN';
  return 'ERROR';
}

/** 
 * Pick a random element from an array.
 * @template T
 * @param {T[]} arr - array to pick from
 * @returns {T} a random element from `arr`
 */
function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

/** 
 * Generate a synthetic log object.
 * Builds a timestamped log with service name, level, message, and requestId.
 * @returns {{timestamp:string,service:string,level:string,message:string,requestId:string}} log object
 */
function generateLog() {
  const level = randomLevel();
  return {
    timestamp: new Date().toISOString(),
    service: randomFrom(services),
    level,
    message: randomFrom(messagesByLevel[level]),
    requestId: Math.random().toString(36).substring(2, 10)
  };
}

/** 
 * Connect the Kafka producer and start sending logs periodically.
 * Connects `producer` then schedules an interval which generates a log
 * and sends it to the `raw-logs` topic every 200ms.
 * @returns {Promise<void>} resolves when the function has started the interval
 */
async function run() {
  await producer.connect();
  console.log('Producer connected. Sending logs every 200ms...');

  // Interval callback: generate a log and send it to Kafka
  setInterval(async () => {
    const log = generateLog();
    await producer.send({
      topic: 'raw-logs',
      messages: [{ value: JSON.stringify(log) }]
    });
    console.log('Sent:', log);
  }, 2000);
}

run().catch(err => {
  console.error('Producer error:', err);
  process.exit(1);
});