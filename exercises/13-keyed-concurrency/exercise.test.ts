import { afterEach, describe, expect, it } from 'vitest';
import type { Consumer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

const keys = ['acc-1', 'acc-2', 'acc-3', 'acc-4', 'acc-5', 'acc-6'];
const perKey = 15;

async function seedTopic(kafka: ReturnType<typeof createKafka>) {
  const topic = await createTopic(kafka, { label: 'ledger' });
  const messages = [];
  for (let seq = 0; seq < perKey; seq++) for (const key of keys) messages.push({ key, value: String(seq) });
  await produce(kafka, topic, messages);
  return topic;
}

/** Her key için: seq n'nin her başarılı işlenmesinden önce seq n-1 en az bir kez başarıyla işlenmiş olmalı. */
function orderViolations(successes: Array<{ key: string; seq: number }>) {
  const violations: string[] = [];
  const seen = new Map<string, Set<number>>();
  for (const { key, seq } of successes) {
    const s = seen.get(key) ?? new Set<number>();
    if (seq > 0 && !s.has(seq - 1)) violations.push(`${key}:${seq} (önceki ${seq - 1} işlenmemişti)`);
    s.add(seq);
    seen.set(key, s);
  }
  return violations;
}

describe('13 - partition içinde key bazlı paralellik', () => {
  const kafka = createKafka();
  const consumers: Consumer[] = [];
  afterEach(async () => {
    await Promise.all(consumers.splice(0).map((c) => c.disconnect()));
  });

  it('key sırası korunur, eşzamanlılık sınırı aşılmaz, iş paralel yapılır', async () => {
    const topic = await seedTopic(kafka);
    const successes: Array<{ key: string; seq: number; at: number }> = [];
    let inFlight = 0;
    let maxInFlight = 0;
    const maxConcurrency = 4;

    consumers.push(
      await impl.startKeyedConsumer(kafka, {
        topic,
        consumerConfig: { groupId: uniqueName('ledger') },
        maxConcurrency,
        handle: async (key, value) => {
          inFlight++;
          maxInFlight = Math.max(maxInFlight, inFlight);
          await sleep(10 + Math.random() * 20);
          inFlight--;
          successes.push({ key, seq: Number(value), at: Date.now() });
        },
      }),
    );
    await waitFor(() => successes.length >= keys.length * perKey, { timeoutMs: 20_000 });

    expect(orderViolations(successes)).toEqual([]);
    expect(maxInFlight, 'eşzamanlılık sınırı aşıldı').toBeLessThanOrEqual(maxConcurrency);
    expect(maxInFlight, 'hiç paralel çalışmadı').toBeGreaterThan(1);
    const duration = successes.at(-1)!.at - successes[0].at;
    expect(duration, 'sıralı işleme ~1.8sn sürer').toBeLessThan(1000);
  });

  it('bir mesaj hata verince kayıp olmaz ve key sırası bozulmaz', async () => {
    const topic = await seedTopic(kafka);
    const successes: Array<{ key: string; seq: number }> = [];
    let failedOnce = false;

    consumers.push(
      await impl.startKeyedConsumer(kafka, {
        topic,
        consumerConfig: { groupId: uniqueName('ledger'), retry: { initialRetryTime: 100, retries: 5 } },
        maxConcurrency: 4,
        handle: async (key, value) => {
          await sleep(5 + Math.random() * 10);
          if (key === 'acc-2' && value === '4' && !failedOnce) {
            failedOnce = true;
            throw new Error('ledger db deadlock');
          }
          successes.push({ key, seq: Number(value) });
        },
      }),
    );
    const allDone = () => keys.every((k) => Array.from({ length: perKey }, (_, i) => i).every((i) => successes.some((s) => s.key === k && s.seq === i)));
    await waitFor(allDone, { timeoutMs: 30_000, message: 'hatadan sonra tüm mesajlar işlenmedi' }).catch(() => undefined);

    expect(failedOnce).toBe(true);
    expect(allDone(), 'kaybolan mesaj var').toBe(true);
    expect(orderViolations(successes)).toEqual([]);
  });
});
