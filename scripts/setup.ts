import { waitForHealthy } from '../src/lib/brokers.js';
import { createKafka } from '../src/lib/kafka.js';
import { createPool } from '../src/lib/db.js';
import { createRedis } from '../src/lib/redis.js';
import { waitFor } from '../src/lib/wait.js';

const kafka = createKafka();

console.log('Kafka cluster bekleniyor (3 broker)...');
await waitForHealthy(kafka, [], 120_000);

console.log('Postgres bekleniyor...');
const pool = createPool(1);
await waitFor(async () => (await pool.query('select 1')).rowCount === 1, { timeoutMs: 60_000, intervalMs: 500 });
await pool.end();

console.log('Redis bekleniyor...');
const redis = createRedis();
await waitFor(async () => (await redis.ping()) === 'PONG', { timeoutMs: 30_000, intervalMs: 500 });
redis.disconnect();

console.log('Hazır. Kafka UI: http://localhost:18080');
