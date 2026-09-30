import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, partitionLeader } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { readAll } from '../../src/lib/messages.js';
import { brokers, waitForHealthy } from '../../src/lib/brokers.js';
import { sleep, waitFor } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('03 - acks ve dayanıklılık', () => {
  const kafka = createKafka();
  const topics: string[] = [];

  afterEach(async () => {
    await brokers.restoreAll(kafka, topics);
  });

  afterAll(async () => {
    await waitForHealthy(kafka);
  });

  it('leader failover sonrası onaylanan (resolve olan) hiçbir mesaj kaybolmaz', async () => {
    const topic = await createTopic(kafka, { label: 'payments' });
    topics.push(topic);
    const admin = kafka.admin();
    await admin.connect();
    const { leader, replicas } = await partitionLeader(admin, topic);
    await admin.disconnect();
    const followers = replicas.filter((r) => r !== leader);

    const producer = kafka.producer();
    await producer.connect();
    // Bağlantı ve metadata ısınsın.
    await impl.durableSend(producer, topic, [{ value: 'warmup' }]);

    await brokers.pause(...followers);
    const acked = new Set<string>();
    const sends = Array.from({ length: 20 }, (_, i) => {
      const value = `payment-${i}`;
      return impl.durableSend(producer, topic, [{ value }]).then(() => void acked.add(value));
    });
    await sleep(1500);
    const ackedBeforeCrash = acked.size;
    await brokers.kill(leader);
    await brokers.unpause(...followers);

    // Eski leader fence edilip yeni leader seçilene kadar bekleyen gönderimler retry eder.
    await Promise.race([Promise.allSettled(sends), sleep(60_000)]);
    await producer.disconnect().catch(() => undefined);

    await brokers.start(leader);
    await waitForHealthy(kafka, [topic]);

    const stored = new Set((await readAll(kafka, topic, { expected: acked.size + 1 })).map((m) => m.value));
    const lost = [...acked].filter((v) => !stored.has(v));

    console.log(`[03] crash öncesi onaylanan: ${ackedBeforeCrash}, toplam onaylanan: ${acked.size}, kaybolan: ${lost.length}`);
    expect(lost, 'onaylandığı halde kaybolan mesajlar').toEqual([]);
  });

  it('ISR < min.insync.replicas iken hata doğru sınıflandırılır', async () => {
    const topic = await createTopic(kafka, { label: 'orders' });
    topics.push(topic);
    const admin = kafka.admin();
    await admin.connect();
    const { leader, replicas } = await partitionLeader(admin, topic);
    const followers = replicas.filter((r) => r !== leader);

    await brokers.stop(...followers);
    // Controller follower'ları fence edip ISR'dan çıkarana kadar bekle.
    await waitFor(async () => (await partitionLeader(admin, topic)).isr.length === 1, {
      timeoutMs: 30_000,
      intervalMs: 500,
      message: 'ISR küçülmedi',
    });
    await admin.disconnect();

    const producer = kafka.producer({ retry: { retries: 1, initialRetryTime: 100 } });
    await producer.connect();

    // acks=1 bu durumda hâlâ "başarılı" döner: tek kopya, leader ölürse kayıp.
    await expect(producer.send({ topic, acks: 1, messages: [{ value: 'fragile' }] })).resolves.toBeDefined();

    const error = await impl.durableSend(producer, topic, [{ value: 'important' }]).then(
      () => undefined,
      (e: unknown) => e,
    );
    await producer.disconnect();

    expect(error, 'durableSend tek kopya varken başarılı dönmemeli').toBeDefined();
    expect((error as { retriable?: boolean }).retriable, 'dıştaki wrapper hata retriable değil: tuzak burada').toBe(false);
    expect(impl.describeFailure(error)).toEqual({ type: 'NOT_ENOUGH_REPLICAS', retriable: true });
  });
});
