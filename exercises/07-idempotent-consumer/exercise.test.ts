import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createPool } from '../../src/lib/db.js';
import { createRedis } from '../../src/lib/redis.js';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import { setupSchema, type DepositEvent } from './schema.js';
import { handleDepositWithRedis } from './redis-dedup.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('07 - idempotent consumer', () => {
  const pool = createPool(20);
  const consumerName = 'ledger-service';

  const newAccount = async () => {
    const id = `acc-${randomUUID().slice(0, 8)}`;
    await pool.query('insert into ex07_accounts (id, balance) values ($1, 0)', [id]);
    return id;
  };
  const balanceOf = async (id: string) =>
    Number((await pool.query('select balance from ex07_accounts where id = $1', [id])).rows[0].balance);
  const deposit = (accountId: string, amount = 100): DepositEvent => ({ messageId: randomUUID(), accountId, amount });

  beforeAll(() => setupSchema(pool));
  afterAll(() => pool.end());

  it('aynı mesaj art arda 3 kez gelirse bakiye bir kez artar', async () => {
    const acc = await newAccount();
    const event = deposit(acc);
    const outcomes = [];
    for (let i = 0; i < 3; i++) outcomes.push(await impl.handleDeposit(pool, consumerName, event));

    expect(outcomes).toEqual(['applied', 'duplicate', 'duplicate']);
    expect(await balanceOf(acc)).toBe(100);
  });

  it('aynı mesaj eşzamanlı 10 kez gelirse bakiye bir kez artar (rebalance sırasında iki consumer)', async () => {
    const acc = await newAccount();
    const event = deposit(acc);
    // Havuzu ısıt: 10 bağlantı hazır olsun ki teslimatlar gerçekten aynı anda yarışsın.
    await Promise.all(Array.from({ length: 10 }, () => pool.query('select pg_sleep(0.05)')));
    const outcomes = await Promise.all(
      Array.from({ length: 10 }, () => impl.handleDeposit(pool, consumerName, event).catch((e: Error) => `error: ${e.message}`)),
    );

    expect(outcomes.filter((o) => o === 'applied'), `sonuçlar: ${JSON.stringify(outcomes)}`).toHaveLength(1);
    expect(outcomes.filter((o) => o !== 'applied' && o !== 'duplicate'), 'duplicate hata değil, normal bir sonuç').toEqual([]);
    expect(await balanceOf(acc)).toBe(100);
  });

  it('işlem yarıda kalırsa retry bakiyeyi tam bir kez artırır', async () => {
    const acc = await newAccount();
    const event = deposit(acc);
    const boom = async () => {
      throw new Error('pod OOM-killed');
    };

    await expect(impl.handleDeposit(pool, consumerName, event, { afterBalanceUpdate: boom })).rejects.toThrow('OOM');
    expect(await impl.handleDeposit(pool, consumerName, event)).toBe('applied');
    expect(await balanceOf(acc)).toBe(100);
  });

  it('farklı consumer’lar aynı mesajı bağımsız işler (dedup consumer başına)', async () => {
    const acc = await newAccount();
    const event = deposit(acc);
    expect(await impl.handleDeposit(pool, 'ledger-service', event)).toBe('applied');
    expect(await impl.handleDeposit(pool, 'ledger-service-v2-migration', event)).toBe('applied');
  });

  it('uçtan uca: producer aynı olayı 3 kez yazdı, consumer bakiyeyi bir kez artırır', async () => {
    const kafka = createKafka();
    const topic = await createTopic(kafka, { label: 'deposits' });
    const acc = await newAccount();
    const event = deposit(acc, 250);
    const msg = { key: acc, value: JSON.stringify(event), headers: { 'message-id': event.messageId } };
    await produce(kafka, topic, [msg, msg, msg]);

    let handled = 0;
    const consumer = kafka.consumer({ groupId: uniqueName('ledger') });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const e: DepositEvent = { ...JSON.parse(message.value!.toString()), messageId: message.headers!['message-id']!.toString() };
        await impl.handleDeposit(pool, consumerName, e);
        handled++;
      },
    });
    await waitFor(() => handled === 3, { timeoutMs: 20_000 });
    await consumer.disconnect();

    expect(await balanceOf(acc)).toBe(250);
  });

  describe('bilgi: Redis SET NX ile dedup (bu testler her zaman geçer)', () => {
    const redis = createRedis();
    afterAll(() => redis.disconnect());

    it('normal akışta çalışıyor gibi görünür', async () => {
      const acc = await newAccount();
      const event = deposit(acc);
      await handleDepositWithRedis(pool, redis, consumerName, event);
      await handleDepositWithRedis(pool, redis, consumerName, event);
      expect(await balanceOf(acc)).toBe(100);
    });

    it('ama crash sonrası mesaj sessizce KAYBOLUR', async () => {
      const acc = await newAccount();
      const event = deposit(acc);
      const boom = async () => {
        throw new Error('crash');
      };
      await expect(handleDepositWithRedis(pool, redis, consumerName, event, { afterDedupMark: boom })).rejects.toThrow();
      await sleep(10);
      // Kafka mesajı tekrar teslim eder (commit edilmedi), ama Redis "zaten işlendi" der.
      expect(await handleDepositWithRedis(pool, redis, consumerName, event)).toBe('duplicate');
      expect(await balanceOf(acc), 'para hiç yatmadı').toBe(0);
    });
  });
});
