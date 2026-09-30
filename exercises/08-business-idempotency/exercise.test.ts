import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createPool } from '../../src/lib/db.js';
import { loadImpl } from '../../src/lib/impl.js';
import { setupSchema, type PaymentCommand } from './schema.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('08 - iş seviyesinde idempotency', () => {
  const pool = createPool(20);
  beforeAll(async () => {
    await setupSchema(pool);
    await impl.ensureSchema(pool);
  });
  afterAll(() => pool.end());

  const command = (overrides: Partial<PaymentCommand> = {}): PaymentCommand => ({
    commandId: randomUUID(),
    idempotencyKey: `idem-${randomUUID().slice(0, 8)}`,
    accountId: `acc-${randomUUID().slice(0, 8)}`,
    amount: 4999,
    currency: 'TRY',
    ...overrides,
  });
  const paymentsOf = async (accountId: string) =>
    (await pool.query('select * from ex08_payments where account_id = $1', [accountId])).rows;

  it('HTTP retry: farklı commandId, aynı key ve içerik: tek ödeme, aynı paymentId döner', async () => {
    const original = command();
    const retry = { ...original, commandId: randomUUID() };

    const first = await impl.processPayment(pool, original);
    const second = await impl.processPayment(pool, retry);

    expect(first.outcome).toBe('created');
    expect(second).toEqual({ outcome: 'duplicate', paymentId: first.paymentId });
    expect(await paymentsOf(original.accountId)).toHaveLength(1);
  });

  it('aynı key farklı içerikle gelirse conflict: ödeme oluşturulmaz', async () => {
    const original = command();
    await impl.processPayment(pool, original);
    const tampered = { ...original, commandId: randomUUID(), amount: 99_999 };

    expect(await impl.processPayment(pool, tampered)).toEqual({ outcome: 'conflict', paymentId: null });
    const rows = await paymentsOf(original.accountId);
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].amount)).toBe(4999);
  });

  it('eşzamanlı retry’lar (kullanıcı butona 5 kez bastı): tek ödeme', async () => {
    const original = command();
    await Promise.all(Array.from({ length: 5 }, () => pool.query('select pg_sleep(0.05)')));
    const results = await Promise.all(
      Array.from({ length: 5 }, () => impl.processPayment(pool, { ...original, commandId: randomUUID() })),
    );

    expect(results.filter((r) => r.outcome === 'created')).toHaveLength(1);
    expect(new Set(results.map((r) => r.paymentId)).size, 'hepsi aynı ödemeyi göstermeli').toBe(1);
    expect(await paymentsOf(original.accountId)).toHaveLength(1);
  });

  it('farklı hesaplar aynı idempotency key’i kullanabilir', async () => {
    const a = command({ idempotencyKey: 'order-1' });
    const b = command({ idempotencyKey: 'order-1' });
    expect((await impl.processPayment(pool, a)).outcome).toBe('created');
    expect((await impl.processPayment(pool, b)).outcome).toBe('created');
  });
});
