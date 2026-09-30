import { afterEach, describe, expect, it } from 'vitest';
import type { Producer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { ApiKey, createChaos } from '../../src/lib/chaos-socket.js';
import { createTopic } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { readAll } from '../../src/lib/messages.js';
import { waitFor } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

const statuses = ['CREATED', 'PAID', 'PACKED', 'SHIPPED', 'DELIVERED'];

async function setup(retries = 5) {
  const chaos = createChaos();
  const kafka = createKafka({ chaos, requestTimeout: 1500 });
  const topic = await createTopic(kafka, { label: 'order-status', partitions: 3 });
  const producer = kafka.producer({ retry: { retries, initialRetryTime: 100 } });
  await producer.connect();
  await producer.send({ topic, messages: [{ key: 'warmup', value: 'warmup' }] });
  return { chaos, kafka, topic, producer };
}

describe('05 - retry altında sıralama', () => {
  const producers: Producer[] = [];
  afterEach(async () => {
    await Promise.all(producers.splice(0).map((p) => p.disconnect().catch(() => undefined)));
  });

  it('aynı key’in eşzamanlı gönderimleri çağrı sırasıyla yazılır', async () => {
    const { chaos, topic, producer } = await setup();
    producers.push(producer);
    const publisher = new impl.OrderedPublisher(producer, topic);

    chaos.swallowRequests(ApiKey.Produce, 1);
    const calls: Promise<void>[] = [];
    for (const status of statuses) {
      for (const order of ['order-a', 'order-b', 'order-c']) calls.push(publisher.publish(order, `${order}:${status}`));
    }
    await Promise.all(calls);

    const messages = await readAll(createKafka(), topic, { expected: 16 });
    for (const order of ['order-a', 'order-b', 'order-c']) {
      const seen = messages.filter((m) => m.key === order).map((m) => m.value!.split(':')[1]);
      expect(seen, `${order} sırası`).toEqual(statuses);
    }
  });

  it('farklı key’ler birbirini beklemez', async () => {
    const { chaos, topic, producer } = await setup();
    producers.push(producer);
    const publisher = new impl.OrderedPublisher(producer, topic);

    const doneAt: Record<string, number> = {};
    const track = (label: string, p: Promise<void>) => p.then(() => void (doneAt[label] = Date.now()));

    // order-slow'un ilk isteği yutulur: ~1.5sn timeout + retry.
    chaos.swallowRequests(ApiKey.Produce, 1);
    const slow = track('slow-1', publisher.publish('order-slow', 'slow-1'));
    await waitFor(() => chaos.events.length === 1, { timeoutMs: 5000, message: 'ilk istek yutulmadı' });
    const fast = ['fast-1', 'fast-2', 'fast-3'].map((v) => track(v, publisher.publish('order-fast', v)));
    await Promise.all([slow, ...fast]);

    expect(doneAt['fast-3'], 'order-fast, order-slow’u beklememeli').toBeLessThan(doneAt['slow-1']);
  });

  it('başarısız gönderim hatayı çağırana döner, key kilitlenmez ve iz bırakmaz', async () => {
    const { chaos, topic, producer } = await setup(0);
    producers.push(producer);
    const publisher = new impl.OrderedPublisher(producer, topic);

    chaos.swallowRequests(ApiKey.Produce, 1);
    const first = publisher.publish('order-x', 'x-1');
    const second = publisher.publish('order-x', 'x-2');

    await expect(first).rejects.toThrow();
    await expect(second).resolves.toBeUndefined();
    await waitFor(() => publisher.pendingKeys() === 0, { timeoutMs: 2000, message: 'bitmiş key’ler temizlenmedi' });

    const values = (await readAll(createKafka(), topic, { expected: 2 })).map((m) => m.value);
    expect(values).toContain('x-2');
  });
});
