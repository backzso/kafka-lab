import { createHash, randomUUID } from 'node:crypto';
import type { Pool } from '../../src/lib/db.js';
import type { PaymentCommand, PaymentResult } from './schema.js';

export async function ensureSchema(pool: Pool): Promise<void> {
  // Idempotency key'in kapsamı hesap: farklı müşteriler aynı key'i üretebilir.
  await pool.query(
    'create unique index if not exists ex08_payments_idem on ex08_payments (account_id, idempotency_key)',
  );
}

/** İsteğin anlamını temsil eden alanlar; commandId gibi taşıma detayları dahil değil. */
function requestHash(cmd: PaymentCommand): string {
  const canonical = JSON.stringify([cmd.accountId, cmd.amount, cmd.currency]);
  return createHash('sha256').update(canonical).digest('hex');
}

export async function processPayment(pool: Pool, cmd: PaymentCommand): Promise<PaymentResult> {
  const hash = requestHash(cmd);
  const inserted = await pool.query<{ payment_id: string }>(
    `insert into ex08_payments (payment_id, account_id, idempotency_key, amount, currency, request_hash)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (account_id, idempotency_key) do nothing
     returning payment_id`,
    [randomUUID(), cmd.accountId, cmd.idempotencyKey, cmd.amount, cmd.currency, hash],
  );
  if (inserted.rowCount) return { outcome: 'created', paymentId: inserted.rows[0].payment_id };

  const existing = await pool.query<{ payment_id: string; request_hash: string }>(
    'select payment_id, request_hash from ex08_payments where account_id = $1 and idempotency_key = $2',
    [cmd.accountId, cmd.idempotencyKey],
  );
  const row = existing.rows[0];
  if (row.request_hash !== hash) return { outcome: 'conflict', paymentId: null };
  return { outcome: 'duplicate', paymentId: row.payment_id };
}
