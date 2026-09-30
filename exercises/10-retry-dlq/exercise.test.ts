import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopics, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce, readAll, type ReadMessage } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import { NonRetriableError, type RetryPipeline } from './types.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('10 - retry topic ve DLQ', () => {
  const kafka = createKafka();
  let pipeline: RetryPipeline | undefined;
  let dlq: ReadMessage[] = [];
  const attempts: Array<{ value: string; attempt: number; at: number; ok: boolean }> = [];
  let startedAt = 0;
  const retryDelayMs = 1000;

  beforeAll(async () => {
    const [main, retry, dlqTopic] = await createTopics(kafka, [
      { label: 'orders' },
      { label: 'orders.retry' },
      { label: 'orders.dlq' },
    ]);

    const flakyFailures = new Map<string, number>();
    pipeline = await impl.startRetryPipeline({
      kafka,
      topics: { main, retry, dlq: dlqTopic },
      groupId: uniqueName('fulfillment'),
      maxAttempts: 3,
      retryDelayMs,
      handler: async (value, attempt) => {
        const record = (ok: boolean) => attempts.push({ value, attempt, at: Date.now(), ok });
        if (value === 'poison') {
          record(false);
          throw new Error('inventory service returned 500');
        }
        if (value === 'invalid') {
          record(false);
          throw new NonRetriableError('schema validation failed: missing sku');
        }
        if (value === 'flaky') {
          const failures = flakyFailures.get(value) ?? 0;
          if (failures < 2) {
            flakyFailures.set(value, failures + 1);
            record(false);
            throw new Error('timeout');
          }
        }
        record(true);
      },
    });

    startedAt = Date.now();
    await produce(kafka, main, ['ok-1', 'poison', 'ok-2', 'flaky', 'invalid', 'ok-3'].map((v) => ({ key: `key-${v}`, value: v })));

    await waitFor(() => attempts.some((a) => a.value === 'flaky' && a.ok), { timeoutMs: 30_000 }).catch(() => undefined);
    await sleep(3000);
    dlq = await readAll(kafka, dlqTopic, { expected: 2, timeoutMs: 8000 });
  });

  afterAll(async () => {
    await pipeline?.stop();
  });

  const of = (value: string) => attempts.filter((a) => a.value === value);

  it('sağlam mesajlar ilk denemede ve poison mesajı beklemeden işlenir', () => {
    for (const v of ['ok-1', 'ok-2', 'ok-3']) {
      expect(of(v).map((a) => a.attempt), v).toEqual([1]);
      expect(of(v)[0].at - startedAt, `${v} geç işlendi: ana akış bloklanmış`).toBeLessThan(retryDelayMs);
    }
  });

  it('geçici hata backoff ile retry edilir ve sonunda başarılı olur', () => {
    const flaky = of('flaky');
    expect(flaky.map((a) => [a.attempt, a.ok])).toEqual([[1, false], [2, false], [3, true]]);
    for (let i = 1; i < flaky.length; i++) {
      expect(flaky[i].at - flaky[i - 1].at, 'denemeler arası bekleme yok').toBeGreaterThanOrEqual(retryDelayMs - 50);
    }
  });

  it('kalıcı hata maxAttempts sonunda DLQ’ya gider; teşhis için header taşır', () => {
    expect(of('poison').map((a) => a.attempt)).toEqual([1, 2, 3]);
    const poison = dlq.filter((m) => m.value === 'poison');
    expect(poison, 'poison DLQ’da tam bir kez olmalı').toHaveLength(1);
    expect(poison[0].key).toBe('key-poison');
    expect(poison[0].headers).toMatchObject({
      attempts: '3',
      error: 'inventory service returned 500',
    });
    expect(poison[0].headers['original-topic']).toMatch(/orders\./);
  });

  it('NonRetriableError retry edilmeden doğrudan DLQ’ya gider', () => {
    expect(of('invalid').map((a) => a.attempt)).toEqual([1]);
    const invalid = dlq.filter((m) => m.value === 'invalid');
    expect(invalid).toHaveLength(1);
    expect(invalid[0].headers).toMatchObject({ attempts: '1', error: 'schema validation failed: missing sku' });
  });

  it('DLQ’da sadece başarısız mesajlar var', () => {
    expect(dlq.map((m) => m.value).sort()).toEqual(['invalid', 'poison']);
  });
});
