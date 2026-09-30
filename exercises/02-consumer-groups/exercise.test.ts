import { afterEach, describe, expect, it } from 'vitest';
import type { Consumer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

/** Tüm consumer'ların grupları rebalance'ı bitirip Stable kalana kadar bekler. */
async function waitUntilStable(consumers: Consumer[]) {
  const allStable = async () =>
    (await Promise.all(consumers.map((c) => c.describeGroup()))).every((g) => g.state === 'Stable');
  await waitFor(async () => {
    if (!(await allStable())) return false;
    await sleep(2000);
    return allStable();
  }, { timeoutMs: 30_000, message: 'grup ataması stabil olmadı' });
}

describe('02 - consumer group temelleri', () => {
  const kafka = createKafka();
  const consumers: Consumer[] = [];

  afterEach(async () => {
    await Promise.all(consumers.splice(0).map((c) => c.disconnect()));
  });

  it('aynı servisin instance’ları işi paylaşır: her mesaj bir kez işlenir', async () => {
    const topic = await createTopic(kafka, { label: 'jobs', partitions: 4 });
    const serviceName = uniqueName('billing-service');
    const counts = { a: 0, b: 0 };

    const a = await impl.startWorker(kafka, { topic, serviceName, instanceId: 'pod-a', onMessage: async () => void counts.a++ });
    consumers.push(a);
    const b = await impl.startWorker(kafka, { topic, serviceName, instanceId: 'pod-b', onMessage: async () => void counts.b++ });
    consumers.push(b);
    await waitUntilStable([a, b]);

    await produce(kafka, topic, Array.from({ length: 40 }, (_, i) => ({ key: `k${i}`, value: String(i) })));
    await waitFor(() => counts.a + counts.b >= 40, { timeoutMs: 20_000 });
    await sleep(1500);

    expect(counts.a + counts.b, 'toplam işlenen mesaj sayısı (duplicate var mı?)').toBe(40);
    expect(counts.a, 'pod-a hiç iş almadı').toBeGreaterThan(0);
    expect(counts.b, 'pod-b hiç iş almadı').toBeGreaterThan(0);
  });

  it('yeni bir grup mevcut mesajları baştan okuyabilir', async () => {
    const topic = await createTopic(kafka, { label: 'history' });
    await produce(kafka, topic, [1, 2, 3, 4, 5].map((i) => ({ value: String(i) })));

    const seen: string[] = [];
    consumers.push(await impl.replayAll(kafka, topic, uniqueName('replayer'), async (m) => void seen.push(m.value!.toString())));
    await waitFor(() => seen.length >= 5, { timeoutMs: 15_000, message: 'mevcut mesajlar okunmadı' }).catch(() => undefined);

    expect(seen).toEqual(['1', '2', '3', '4', '5']);
  });

  it('grup offset’i timestamp’e geri sarılır, sadece o andan sonraki mesajlar yeniden işlenir', async () => {
    const topic = await createTopic(kafka, { label: 'rewind', partitions: 2 });
    const groupId = uniqueName('rewind-group');

    await produce(kafka, topic, ['a1', 'a2', 'a3'].map((v) => ({ key: v, value: v })));
    await sleep(1000);
    const rewindTo = Date.now();
    await sleep(200);
    await produce(kafka, topic, ['b1', 'b2', 'b3'].map((v) => ({ key: v, value: v })));

    const consumeAll = async (expected: number) => {
      const seen: string[] = [];
      const c = kafka.consumer({ groupId, maxWaitTimeInMs: 200 });
      await c.connect();
      await c.subscribe({ topic, fromBeginning: true });
      await c.run({ eachMessage: async ({ message }) => void seen.push(message.value!.toString()) });
      await waitFor(() => seen.length >= expected, { timeoutMs: 20_000 }).catch(() => undefined);
      await sleep(1000);
      return { seen, consumer: c };
    };

    const first = await consumeAll(6);
    expect(first.seen.sort()).toEqual(['a1', 'a2', 'a3', 'b1', 'b2', 'b3']);

    // Grup aktifken geri sarma reddedilmeli.
    await expect(impl.rewindGroupToTimestamp(kafka, groupId, topic, rewindTo)).rejects.toThrow(/active/i);
    await first.consumer.disconnect();

    await impl.rewindGroupToTimestamp(kafka, groupId, topic, rewindTo);

    const second = await consumeAll(3);
    await second.consumer.disconnect();
    expect(second.seen.sort()).toEqual(['b1', 'b2', 'b3']);
  });
});
