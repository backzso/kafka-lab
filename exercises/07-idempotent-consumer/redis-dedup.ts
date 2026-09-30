import type { Pool } from '../../src/lib/db.js';
import type { Redis } from '../../src/lib/redis.js';
import type { DepositEvent, Faults, Outcome } from './schema.js';

/**
 * Sık görülen bir yaklaşım: dedup'ı Redis SET NX ile yapmak. Bu bir egzersiz değil, testte incelenen örnek.
 * Sorun: Redis ve Postgres ayrı sistemler. Dedup kaydı ile iş etkisi atomik değil.
 */
export async function handleDepositWithRedis(
  pool: Pool,
  redis: Redis,
  consumerName: string,
  event: DepositEvent,
  faults: Faults = {},
): Promise<Outcome> {
  const first = await redis.set(`dedup:${consumerName}:${event.messageId}`, '1', 'EX', 86_400, 'NX');
  if (first !== 'OK') return 'duplicate';

  await faults.afterDedupMark?.();
  await pool.query('update ex07_accounts set balance = balance + $2 where id = $1', [event.accountId, event.amount]);
  return 'applied';
}
