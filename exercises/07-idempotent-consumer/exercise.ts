import type { Pool } from '../../src/lib/db.js';
import type { DepositEvent, Faults, Outcome } from './schema.js';

/**
 * Para yatırma olayını işler. Aynı messageId kaç kez, hangi sırayla ya da ne kadar eşzamanlı
 * gelirse gelsin bakiye tam olarak bir kez artmalı.
 *
 * Tablolar: ex07_accounts(id, balance), ex07_processed_messages(consumer, message_id) PK(consumer, message_id)
 */
export async function handleDeposit(
  pool: Pool,
  consumerName: string,
  event: DepositEvent,
  faults: Faults = {},
): Promise<Outcome> {
  // TODO: Bu kodda en az iki problem var. Eşzamanlı teslimat? Yarıda kalan işlem?
  const seen = await pool.query(
    'select 1 from ex07_processed_messages where consumer = $1 and message_id = $2',
    [consumerName, event.messageId],
  );
  if (seen.rowCount) return 'duplicate';

  await pool.query('update ex07_accounts set balance = balance + $2 where id = $1', [event.accountId, event.amount]);
  await faults.afterBalanceUpdate?.();
  await pool.query('insert into ex07_processed_messages (consumer, message_id) values ($1, $2)', [
    consumerName,
    event.messageId,
  ]);
  return 'applied';
}
