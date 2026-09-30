import type { Pool } from '../../src/lib/db.js';

/** Bu egzersizin tabloları. Testler her çalıştırmada kendi account id'lerini kullanır. */
export async function setupSchema(pool: Pool) {
  await pool.query(`
    create table if not exists ex07_accounts (
      id text primary key,
      balance bigint not null default 0
    );
    create table if not exists ex07_processed_messages (
      consumer text not null,
      message_id text not null,
      processed_at timestamptz not null default now(),
      primary key (consumer, message_id)
    );
  `);
}

export interface DepositEvent {
  /** Producer tarafında üretilen benzersiz mesaj kimliği (header: message-id) */
  messageId: string;
  accountId: string;
  amount: number;
}

export interface Faults {
  /** Bakiye güncellendikten hemen sonra çağrılır; test burada hata fırlatarak yarım kalan işlemi simüle eder. */
  afterBalanceUpdate?: () => Promise<void>;
  /** Sadece redis-dedup.ts: dedup işareti konduktan sonra, iş etkisinden önce. */
  afterDedupMark?: () => Promise<void>;
}

export type Outcome = 'applied' | 'duplicate';
