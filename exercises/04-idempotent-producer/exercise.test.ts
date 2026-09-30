import { afterEach, describe, expect, it } from 'vitest';
import type { Producer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { ApiKey, createChaos } from '../../src/lib/chaos-socket.js';
import { createTopic } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { readAll } from '../../src/lib/messages.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

const count = (values: (string | null)[], v: string) => values.filter((x) => x === v).length;

describe('04 - idempotent producer', () => {
  const producers: Producer[] = [];
  afterEach(async () => {
    await Promise.all(producers.splice(0).map((p) => p.disconnect().catch(() => undefined)));
  });

  it('yanıt kaybolup retry edilince mesaj bir kez yazılır', async () => {
    const chaos = createChaos();
    const kafka = createKafka({ chaos });
    const topic = await createTopic(kafka, { label: 'payments' });

    const producer = impl.createPaymentProducer(kafka);
    producers.push(producer);
    await producer.connect();
    await producer.send({ topic, messages: [{ value: 'warmup' }] });

    chaos.dropResponses(ApiKey.Produce, 1);
    await producer.send({ topic, messages: [{ key: 'order-42', value: 'charge-42' }] });

    expect(chaos.events, 'chaos tetiklenmedi').toHaveLength(1);
    const values = (await readAll(createKafka(), topic, { expected: 2 })).map((m) => m.value);
    expect(count(values, 'charge-42'), `topic içeriği: ${JSON.stringify(values)}`).toBe(1);
  });

  it('art arda iki kez yanıt kaybolsa da batch bir kez yazılır', async () => {
    const chaos = createChaos();
    const kafka = createKafka({ chaos });
    const topic = await createTopic(kafka, { label: 'payments' });

    const producer = impl.createPaymentProducer(kafka);
    producers.push(producer);
    await producer.connect();
    await producer.send({ topic, messages: [{ value: 'warmup' }] });

    chaos.dropResponses(ApiKey.Produce, 2);
    const batch = ['charge-1', 'charge-2', 'charge-3'];
    await producer.send({ topic, messages: batch.map((value) => ({ key: 'order-7', value })) });

    const values = (await readAll(createKafka(), topic, { expected: 4 })).map((m) => m.value);
    expect(values).toEqual(['warmup', ...batch]);
  });

  it('bilgi: idempotence producer oturumu ile sınırlıdır (bu test her zaman geçer)', async () => {
    const kafka = createKafka();
    const topic = await createTopic(kafka, { label: 'payments' });

    // Uygulama gönderdi, ack'i alamadan çöktü; yeniden başlayınca aynı ödemeyi tekrar gönderdi.
    for (let attempt = 0; attempt < 2; attempt++) {
      const producer = impl.createPaymentProducer(kafka);
      await producer.connect();
      await producer.send({ topic, messages: [{ key: 'order-9', value: 'charge-9' }] });
      await producer.disconnect();
    }

    const values = (await readAll(kafka, topic, { expected: 2 })).map((m) => m.value);
    // Yeni producer = yeni PID: broker bunu duplicate olarak tanımaz. Çözüm consumer tarafında (07/08).
    expect(count(values, 'charge-9')).toBe(2);
  });
});
