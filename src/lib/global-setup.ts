import { createKafka } from './kafka.js';
import { createPool } from './db.js';
import { createRedis } from './redis.js';
import { deleteLabTopics } from './topics.js';

const hint = 'Lab ortamı ayakta mı? `npm run infra:up` çalıştır.';

export async function setup() {
  const kafka = createKafka();
  const admin = kafka.admin();
  try {
    await admin.connect();
    const cluster = await admin.describeCluster();
    if (cluster.brokers.length !== 3) {
      console.warn(`[kafka-lab] Uyarı: ${cluster.brokers.length}/3 broker görünüyor. Önceki bir chaos testi yarıda kalmış olabilir: \`docker compose start\``);
    }
  } catch (e) {
    throw new Error(`Kafka'ya bağlanılamadı. ${hint}\n${(e as Error).message}`);
  } finally {
    await admin.disconnect().catch(() => undefined);
  }

  const pool = createPool(1);
  try {
    await pool.query('select 1');
  } catch (e) {
    throw new Error(`Postgres'e bağlanılamadı. ${hint}\n${(e as Error).message}`);
  } finally {
    await pool.end();
  }

  const redis = createRedis();
  try {
    await redis.ping();
  } catch (e) {
    throw new Error(`Redis'e bağlanılamadı. ${hint}\n${(e as Error).message}`);
  } finally {
    redis.disconnect();
  }
}

export async function teardown() {
  if (process.env.KEEP_TOPICS) return;
  await deleteLabTopics(createKafka()).catch(() => undefined);
}
