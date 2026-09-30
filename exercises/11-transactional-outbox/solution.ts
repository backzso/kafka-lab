import { withTransaction, type Pool } from '../../src/lib/db.js';
import { sleep } from '../../src/lib/wait.js';
import type { CreateOrderFaults, Order, Relay, RelayOptions } from './schema.js';

export async function createOrder(pool: Pool, order: Order, faults: CreateOrderFaults = {}): Promise<void> {
  await withTransaction(pool, async (client) => {
    await client.query('insert into ex11_orders (id, customer_id, total) values ($1, $2, $3)', [
      order.id,
      order.customerId,
      order.total,
    ]);
    await faults.afterOrderInsert?.();
    await client.query(`insert into ex11_outbox (aggregate_id, event_type, payload) values ($1, 'OrderCreated', $2)`, [
      order.id,
      JSON.stringify(order),
    ]);
  });
}

/** Tüm relay instance'ları arasında paylaşılan kilit anahtarı. */
const RELAY_LOCK_KEY = 11_000_001;

export function startOutboxRelay(opts: RelayOptions): Relay {
  const { pool, producer, topic, batchSize, pollIntervalMs, faults = {} } = opts;
  let running = true;

  /** Bir batch yayınlar; yayınlanan satır sayısını döner. */
  const publishBatch = () =>
    withTransaction(pool, async (client) => {
      // Aynı anda tek aktif relay: SKIP LOCKED ile paralel çalışan relay'ler aynı aggregate'in olaylarını
      // farklı batch'lerde, yarışarak yayınlayıp sırayı bozabilir. Transaction bitince kilit kendiliğinden bırakılır.
      const { rows: lock } = await client.query('select pg_try_advisory_xact_lock($1) as ok', [RELAY_LOCK_KEY]);
      if (!lock[0].ok) return 0;

      const { rows } = await client.query(
        'select id, aggregate_id, event_type, payload from ex11_outbox where published_at is null order by id limit $1',
        [batchSize],
      );
      if (!rows.length) return 0;

      await producer.send({
        topic,
        messages: rows.map((r) => ({
          key: r.aggregate_id,
          value: JSON.stringify({ type: r.event_type, payload: r.payload }),
          headers: { 'message-id': String(r.id) },
        })),
      });
      // Buradan sonra crash: satırlar işaretlenmez, bir sonraki batch aynı olayları tekrar yayınlar.
      // Aynı message-id taşıdıkları için consumer tarafında dedup edilebilirler (bkz. 07).
      await faults.afterPublishBeforeMark?.();
      await client.query('update ex11_outbox set published_at = now() where id = any($1)', [rows.map((r) => r.id)]);
      return rows.length;
    });

  const loop = (async () => {
    while (running) {
      const published = await publishBatch().catch(() => 0);
      if (published === 0) await sleep(pollIntervalMs);
    }
  })();

  return {
    async stop() {
      running = false;
      await loop;
    },
  };
}
