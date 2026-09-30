import type { Pool } from '../../src/lib/db.js';
import { sleep } from '../../src/lib/wait.js';
import type { CreateOrderFaults, Order, Relay, RelayOptions } from './schema.js';

/** Siparişi kaydeder ve OrderCreated olayını outbox'a yazar. İkisi birlikte olmalı ya da hiç olmamalı. */
export async function createOrder(pool: Pool, order: Order, faults: CreateOrderFaults = {}): Promise<void> {
  // TODO: Crash olursa?
  await pool.query('insert into ex11_orders (id, customer_id, total) values ($1, $2, $3)', [
    order.id,
    order.customerId,
    order.total,
  ]);
  await faults.afterOrderInsert?.();
  await pool.query(`insert into ex11_outbox (aggregate_id, event_type, payload) values ($1, 'OrderCreated', $2)`, [
    order.id,
    JSON.stringify(order),
  ]);
}

/**
 * Outbox'taki yayınlanmamış olayları sırayla Kafka'ya yazar ve published işaretler.
 * Birden fazla relay instance'ı aynı anda çalışabilir (ör. 2 pod).
 */
export function startOutboxRelay(opts: RelayOptions): Relay {
  const { pool, producer, topic, batchSize, pollIntervalMs, faults = {} } = opts;
  let running = true;

  const loop = (async () => {
    while (running) {
      try {
        // TODO: İki relay aynı anda bu sorguyu çalıştırırsa?
        const { rows } = await pool.query(
          'select id, aggregate_id, event_type, payload from ex11_outbox where published_at is null order by id limit $1',
          [batchSize],
        );
        if (!rows.length) {
          await sleep(pollIntervalMs);
          continue;
        }
        await producer.send({
          topic,
          messages: rows.map((r) => ({
            key: r.aggregate_id,
            value: JSON.stringify({ type: r.event_type, payload: r.payload }),
            headers: { 'message-id': String(r.id) },
          })),
        });
        await faults.afterPublishBeforeMark?.();
        await pool.query('update ex11_outbox set published_at = now() where id = any($1)', [rows.map((r) => r.id)]);
      } catch (e) {
        await sleep(pollIntervalMs);
      }
    }
  })();

  return {
    async stop() {
      running = false;
      await loop;
    },
  };
}
