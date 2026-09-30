import { afterEach, describe, expect, it } from 'vitest';
import type { Consumer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import { DownstreamUnavailableError } from './types.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('14 - backpressure ve lag', () => {
  const kafka = createKafka();
  const consumers: Consumer[] = [];
  afterEach(async () => {
    await Promise.all(consumers.splice(0).map((c) => c.disconnect()));
  });

  it('downstream kesintisinde mesaj kaybolmaz, sıra bozulmaz, downstream yorulmaz', async () => {
    const topic = await createTopic(kafka, { label: 'webhooks' });
    const values = Array.from({ length: 20 }, (_, i) => `hook-${i}`);
    const delivered: string[] = [];
    let callsDuringOutage = 0;
    let outageUntil = 0;
    const retryAfterMs = 500;
    const outageMs = 3000;

    consumers.push(
      await impl.startResilientConsumer(kafka, {
        topic,
        consumerConfig: { groupId: uniqueName('webhook-dispatcher'), maxWaitTimeInMs: 100, retry: { retries: 1000, initialRetryTime: 50, maxRetryTime: 200 } },
        sink: async (value) => {
          if (value === 'hook-5' && outageUntil === 0) outageUntil = Date.now() + outageMs;
          if (Date.now() < outageUntil) {
            callsDuringOutage++;
            throw new DownstreamUnavailableError(retryAfterMs);
          }
          delivered.push(value);
        },
      }),
    );
    await produce(kafka, topic, values.map((value) => ({ value })));

    await waitFor(() => delivered.length >= values.length, { timeoutMs: 20_000 }).catch(() => undefined);
    await sleep(500);

    expect(delivered, 'kayıp ya da sıra bozulması').toEqual(values);
    const maxReasonableCalls = Math.ceil(outageMs / retryAfterMs) + 2;
    expect(callsDuringOutage, `kesinti sırasında downstream ${callsDuringOutage} kez denendi (retryAfter’a uyulmadı)`).toBeLessThanOrEqual(maxReasonableCalls);
  });

  it('consumer lag doğru hesaplanır (commit yok, kısmi, tamamı)', async () => {
    const topic = await createTopic(kafka, { label: 'lag', partitions: 2 });
    const groupId = uniqueName('lag-group');
    await produce(kafka, topic, Array.from({ length: 10 }, (_, i) => ({ partition: i % 2, value: String(i) })));

    expect(await impl.getConsumerLag(kafka, groupId, topic), 'henüz commit yok: tüm mesajlar lag').toBe(10);

    const admin = kafka.admin();
    await admin.connect();
    await admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset: '3' }, { partition: 1, offset: '1' }] });
    await admin.disconnect();
    expect(await impl.getConsumerLag(kafka, groupId, topic), 'p0: 5-3, p1: 5-1').toBe(6);

    let seen = 0;
    const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100 });
    consumers.push(consumer);
    await consumer.connect();
    await consumer.subscribe({ topic });
    await consumer.run({ eachMessage: async () => void seen++ });
    await waitFor(() => seen >= 6, { timeoutMs: 15_000 });
    await waitFor(async () => (await impl.getConsumerLag(kafka, groupId, topic)) === 0, { timeoutMs: 10_000, message: 'lag 0 olmadı' });
  });
});
