import { randomUUID } from 'node:crypto';
import type { Pool } from '../../src/lib/db.js';
import type { PaymentCommand, PaymentResult } from './schema.js';

/** Test başında bir kez çağrılır. Ek index/constraint gerekiyorsa burada oluştur (idempotent olmalı). */
export async function ensureSchema(pool: Pool): Promise<void> {
  // TODO
}

/**
 * Ödeme komutunu işler.
 * - Aynı hesap + aynı idempotencyKey + aynı içerik: yeni ödeme yok, orijinal paymentId ile 'duplicate'.
 * - Aynı hesap + aynı idempotencyKey + farklı içerik: 'conflict' (istemci hatası), ödeme yok.
 * - Farklı hesaplar aynı key'i kullanabilir.
 */
export async function processPayment(pool: Pool, cmd: PaymentCommand): Promise<PaymentResult> {
  // TODO: 07'deki mesaj bazlı dedup burada yeterli mi?
  const paymentId = randomUUID();
  await pool.query(
    `insert into ex08_payments (payment_id, account_id, idempotency_key, amount, currency, request_hash)
     values ($1, $2, $3, $4, $5, '')`,
    [paymentId, cmd.accountId, cmd.idempotencyKey, cmd.amount, cmd.currency],
  );
  return { outcome: 'created', paymentId };
}
