import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Consumer, Producer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import type { CustomerEvent } from './types.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('01 - partition ve key', () => {
  const kafka = createKafka();
  let producer: Producer;
  let consumer: Consumer | undefined;
  let topic: string;

  // 5 müşteri / 6 partition: key'siz round-robin dağıtımın tesadüfen doğru görünmesini engeller.
  const customers = ['alice', 'bob', 'carol', 'dave', 'erin'];
  const events: CustomerEvent[] = [];
  for (let seq = 0; seq < 12; seq++) {
    for (const c of customers) {
      events.push({ customerId: c, orderId: `${c}-order-${seq % 3}`, seq, type: 'ORDER_UPDATED' });
    }
  }

  const processed: Array<CustomerEvent & { partition: number; at: number }> = [];

  beforeAll(async () => {
    topic = await createTopic(kafka, { label: 'customer-events', partitions: 6 });
    producer = kafka.producer();
    await producer.connect();
    // Tek tek gönder: her olay ayrı bir istek, sıra korunmalı.
    for (const e of events) await impl.publishCustomerEvents(producer, topic, [e]);

    consumer = await impl.startCustomerConsumer(kafka, topic, uniqueName('g'), async (e, partition) => {
      // Rastgele gecikme: partition'lar arası sıralama garantisi olmadığını görünür kılar.
      await sleep(30 + Math.random() * 40);
      processed.push({ ...e, partition, at: Date.now() });
    });
    await waitFor(() => processed.length >= events.length, { timeoutMs: 30_000, message: 'tüm mesajlar işlenmedi' });
  });

  afterAll(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('aynı müşterinin tüm olayları tek bir partition’da', () => {
    for (const c of customers) {
      const partitions = new Set(processed.filter((p) => p.customerId === c).map((p) => p.partition));
      expect(partitions.size, `${c} olayları ${[...partitions].join(',')} partition'larına dağılmış`).toBe(1);
    }
  });

  it('her müşteri için olaylar seq sırasıyla işlendi', () => {
    for (const c of customers) {
      const seqs = processed.filter((p) => p.customerId === c).map((p) => p.seq);
      expect(seqs, `${c} sırası bozuk`).toEqual([...seqs].sort((a, b) => a - b));
    }
  });

  it('partition’lar paralel işlendi (60 mesaj < 2sn)', () => {
    const duration = processed.at(-1)!.at - processed[0].at;
    const perPartition = new Map<number, number>();
    for (const p of processed) perPartition.set(p.partition, (perPartition.get(p.partition) ?? 0) + 1);
    expect(duration, `partition dağılımı: ${JSON.stringify([...perPartition])}`).toBeLessThan(2000);
  });
});
