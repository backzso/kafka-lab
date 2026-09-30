import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Producer } from 'kafkajs';
import { createPool } from '../../src/lib/db.js';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { readAll, type ReadMessage } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import { resetTables, setupSchema, type Order, type Relay } from './schema.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('11 - transactional outbox', () => {
  const pool = createPool(20);
  const kafka = createKafka();
  let producer: Producer;
  const relays: Relay[] = [];

  const order = (overrides: Partial<Order> = {}): Order => ({
    id: `order-${randomUUID().slice(0, 8)}`,
    customerId: 'cust-1',
    total: 1500,
    ...overrides,
  });
  const countRows = async (table: string, where = 'true') =>
    Number((await pool.query(`select count(*) from ${table} where ${where}`)).rows[0].count);
  const unpublished = () => countRows('ex11_outbox', 'published_at is null');

  /** Aynı aggregate için ek olaylar (OrderPaid, OrderShipped, ...) ekler. */
  const appendEvents = async (aggregateId: string, types: string[]) => {
    for (const type of types) {
      await pool.query('insert into ex11_outbox (aggregate_id, event_type, payload) values ($1, $2, $3)', [
        aggregateId,
        type,
        JSON.stringify({ orderId: aggregateId }),
      ]);
    }
  };

  beforeAll(async () => {
    await setupSchema(pool);
    producer = kafka.producer({ idempotent: true });
    await producer.connect();
  });
  beforeEach(() => resetTables(pool));
  afterEach(async () => {
    await Promise.all(relays.splice(0).map((r) => r.stop()));
  });
  afterAll(async () => {
    await producer.disconnect();
    await pool.end();
  });

  describe('createOrder', () => {
    it('sipariş ve olay birlikte yazılır', async () => {
      const o = order();
      await impl.createOrder(pool, o);
      expect(await countRows('ex11_orders', `id = '${o.id}'`)).toBe(1);
      expect(await countRows('ex11_outbox', `aggregate_id = '${o.id}' and event_type = 'OrderCreated'`)).toBe(1);
    });

    it('arada crash olursa ikisi de yazılmaz (olaysız sipariş yok)', async () => {
      const o = order();
      const crash = async () => {
        throw new Error('process crashed');
      };
      await expect(impl.createOrder(pool, o, { afterOrderInsert: crash })).rejects.toThrow();
      expect(await countRows('ex11_orders', `id = '${o.id}'`), 'sipariş kaldı ama olayı yok').toBe(0);
      expect(await countRows('ex11_outbox')).toBe(0);
    });
  });

  describe('relay', () => {
    const parse = (m: ReadMessage) => ({ id: m.headers['message-id'], key: m.key, ...JSON.parse(m.value!) });

    it('bekleyen olayları message-id ile sırayla yayınlar ve işaretler', async () => {
      const topic = await createTopic(kafka, { label: 'order-events', partitions: 3 });
      const o = order();
      await impl.createOrder(pool, o);
      await appendEvents(o.id, ['OrderPaid', 'OrderShipped']);

      relays.push(impl.startOutboxRelay({ pool, producer, topic, batchSize: 10, pollIntervalMs: 100 }));
      await waitFor(async () => (await unpublished()) === 0, { timeoutMs: 15_000, message: 'olaylar işaretlenmedi' });

      const events = (await readAll(kafka, topic, { expected: 3 })).map(parse);
      expect(events.map((e) => [e.id, e.key, e.type])).toEqual([
        ['1', o.id, 'OrderCreated'],
        ['2', o.id, 'OrderPaid'],
        ['3', o.id, 'OrderShipped'],
      ]);
      expect(events[0].payload).toMatchObject({ id: o.id, total: 1500 });
    });

    it('yayınla-işaretle arasında crash: olay kaybolmaz, tekrarlar aynı message-id’yi taşır', async () => {
      const topic = await createTopic(kafka, { label: 'order-events' });
      for (let i = 0; i < 5; i++) await impl.createOrder(pool, order());

      let crashes = 0;
      relays.push(
        impl.startOutboxRelay({
          pool,
          producer,
          topic,
          batchSize: 3,
          pollIntervalMs: 100,
          faults: {
            afterPublishBeforeMark: async () => {
              if (crashes++ === 0) throw new Error('relay pod killed');
            },
          },
        }),
      );
      await waitFor(async () => (await unpublished()) === 0, { timeoutMs: 15_000, message: 'olaylar işaretlenmedi' });

      const events = (await readAll(kafka, topic, { expected: 8 })).map(parse);
      const ids = events.map((e) => e.id);
      expect(new Set(ids), 'her olay en az bir kez yayınlanmalı').toEqual(new Set(['1', '2', '3', '4', '5']));
      expect(ids.length, 'crash edilen batch tekrar yayınlanmalı (at-least-once)').toBe(8);
    });

    it('iki relay aynı anda çalışır: her olay bir kez, aggregate sırası korunur', async () => {
      const topic = await createTopic(kafka, { label: 'order-events', partitions: 3 });
      const aggregates = ['order-a', 'order-b', 'order-c'];
      const steps = ['Created', 'Paid', 'Packed', 'Shipped', 'OutForDelivery', 'Delivered', 'Reviewed', 'Closed'];
      const total = aggregates.length * steps.length;

      // Her pod'un ağ gecikmesi farklı: gönderimler rastgele 0-60ms gecikir.
      const jittery = new Proxy(producer, {
        get(target, prop, receiver) {
          if (prop === 'send') {
            return async (record: Parameters<Producer['send']>[0]) => {
              await sleep(Math.random() * 60);
              return target.send(record);
            };
          }
          return Reflect.get(target, prop, receiver);
        },
      });
      relays.push(impl.startOutboxRelay({ pool, producer: jittery, topic, batchSize: 2, pollIntervalMs: 20 }));
      relays.push(impl.startOutboxRelay({ pool, producer: jittery, topic, batchSize: 2, pollIntervalMs: 20 }));
      for (const step of steps) {
        for (const agg of aggregates) await appendEvents(agg, [`Order${step}`]);
        await sleep(15);
      }
      await waitFor(async () => (await unpublished()) === 0, { timeoutMs: 20_000, message: 'olaylar işaretlenmedi' });

      const events = (await readAll(kafka, topic, { expected: total })).map(parse);
      const ids = events.map((e) => e.id);
      expect(ids.length, `duplicate yayın: ${ids.join(',')}`).toBe(new Set(ids).size);
      expect(ids.length).toBe(total);
      for (const agg of aggregates) {
        const types = events.filter((e) => e.key === agg).map((e) => e.type);
        expect(types, `${agg} sırası`).toEqual(steps.map((s) => `Order${s}`));
      }
    });
  });
});
