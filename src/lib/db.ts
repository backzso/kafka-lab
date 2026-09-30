import pg from 'pg';
import { config } from './config.js';

export type Pool = pg.Pool;
export type PoolClient = pg.PoolClient;

export function createPool(max = 10): pg.Pool {
  return new pg.Pool({ connectionString: config.pgUrl, max });
}

/** Postgres unique_violation hata kodu. */
export const UNIQUE_VIOLATION = '23505';

export function isUniqueViolation(e: unknown): boolean {
  return typeof e === 'object' && e !== null && (e as { code?: string }).code === UNIQUE_VIOLATION;
}

/** client.query('BEGIN') ... COMMIT/ROLLBACK kalıbı. */
export async function withTransaction<T>(pool: pg.Pool, fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    client.release();
  }
}
