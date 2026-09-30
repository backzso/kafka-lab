import type { Pool } from '../../src/lib/db.js';

export async function setupSchema(pool: Pool) {
  await pool.query(`
    create table if not exists ex11_orders (
      id text primary key,
      customer_id text not null,
      total bigint not null check (total > 0)
    );
    create table if not exists ex11_outbox (
      id bigserial primary key,
      aggregate_id text not null,
      event_type text not null,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      published_at timestamptz
    );
  `);
}

export async function resetTables(pool: Pool) {
  await pool.query('truncate ex11_orders, ex11_outbox restart identity');
}

export interface Order {
  id: string;
  customerId: string;
  total: number;
}

export interface CreateOrderFaults {
  /** Sipariş satırı yazıldıktan sonra, event yazılmadan önce. */
  afterOrderInsert?: () => Promise<void>;
}

export interface RelayFaults {
  /** Kafka'ya gönderildikten sonra, satırlar published işaretlenmeden önce. */
  afterPublishBeforeMark?: () => Promise<void>;
}

export interface RelayOptions {
  pool: Pool;
  producer: import('kafkajs').Producer;
  topic: string;
  batchSize: number;
  pollIntervalMs: number;
  faults?: RelayFaults;
}

export interface Relay {
  stop(): Promise<void>;
}

/**
 * Relay'in Kafka'ya yazdığı mesaj sözleşmesi (test bunu kontrol eder):
 * - key: aggregate_id
 * - value: JSON { type: event_type, payload }
 * - header `message-id`: outbox satırının id'si (string)
 */
export interface PublishedEvent {
  type: string;
  payload: Record<string, unknown>;
}
