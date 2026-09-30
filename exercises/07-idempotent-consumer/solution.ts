import { withTransaction, type Pool } from '../../src/lib/db.js';
import type { DepositEvent, Faults, Outcome } from './schema.js';

export async function handleDeposit(
  pool: Pool,
  consumerName: string,
  event: DepositEvent,
  faults: Faults = {},
): Promise<Outcome> {
  return withTransaction(pool, async (client) => {
    // Önce dedup kaydını "claim" et. Eşzamanlı ikinci transaction unique index üzerinde bekler,
    // ilki commit edince ON CONFLICT ile boş döner. Check-then-act yarışı yok.
    const claimed = await client.query(
      `insert into ex07_processed_messages (consumer, message_id) values ($1, $2)
       on conflict do nothing returning 1`,
      [consumerName, event.messageId],
    );
    if (!claimed.rowCount) return 'duplicate';

    await client.query('update ex07_accounts set balance = balance + $2 where id = $1', [event.accountId, event.amount]);
    // Buradaki bir hata tüm transaction'ı (dedup kaydı dahil) geri alır; retry temiz başlar.
    await faults.afterBalanceUpdate?.();
    return 'applied';
  });
}
