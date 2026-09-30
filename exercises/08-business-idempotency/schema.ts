import type { Pool } from '../../src/lib/db.js';

export async function setupSchema(pool: Pool) {
  // Her çalıştırmada sıfırdan: önceki denemelerin satırları ve index'leri (çözüm dahil) sonucu etkilemesin.
  await pool.query(`
    drop table if exists ex08_payments;
    create table ex08_payments (
      payment_id text primary key,
      account_id text not null,
      idempotency_key text not null,
      amount bigint not null,
      currency text not null,
      request_hash text not null,
      created_at timestamptz not null default now()
    );
  `);
}

/** API'den gelen ödeme komutu. Kafka'ya her HTTP isteği için ayrı mesaj olarak yazılır. */
export interface PaymentCommand {
  /** Mesaj kimliği: her HTTP isteğinde yenidir (retry'da da). */
  commandId: string;
  /** İstemcinin gönderdiği Idempotency-Key header'ı: retry'da aynı kalır. */
  idempotencyKey: string;
  accountId: string;
  amount: number;
  currency: string;
}

export interface PaymentResult {
  outcome: 'created' | 'duplicate' | 'conflict';
  /** created ve duplicate için ödeme id'si (duplicate'te orijinal ödemenin id'si). conflict'te null. */
  paymentId: string | null;
}
