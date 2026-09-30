import { afterEach, describe, expect, it } from 'vitest';
import type { Consumer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { produce } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import type { Job } from './exercise.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

describe('09 - rebalance kaynaklı duplicate', () => {
  const kafka = createKafka();
  const consumers: Consumer[] = [];
  afterEach(async () => {
    await Promise.all(consumers.splice(0).map((c) => c.disconnect().catch(() => undefined)));
  });

  it('sessionTimeout’tan uzun süren her iş tam bir kez çalışır', async () => {
    // Her partition'ın leader'ı farklı broker: consumer'ın tüm fetcher'ları meşgul olur (bkz. README).
    const topic = await createTopic(kafka, { label: 'report-jobs', replicaAssignment: [[1, 2, 3], [2, 3, 1], [3, 1, 2]] });
    const starts: Array<{ job: string; at: number }> = [];
    const done = new Set<string>();

    const consumer = await impl.startJobWorker(kafka, {
      topic,
      consumerConfig: {
        groupId: uniqueName('reports'),
        sessionTimeout: 3000,
        heartbeatInterval: 500,
        rebalanceTimeout: 10_000,
        maxWaitTimeInMs: 200,
      },
      runJob: async (job) => {
        starts.push({ job: job.id, at: Date.now() });
        await sleep(job.durationMs);
        done.add(job.id);
      },
    });
    consumers.push(consumer);

    const jobs: Job[] = ['daily', 'monthly', 'yearly'].map((id) => ({ id: `${id}-report`, durationMs: 4500 }));
    await produce(kafka, topic, jobs.map((j, partition) => ({ partition, value: JSON.stringify(j) })));

    await waitFor(() => done.size === jobs.length, { timeoutMs: 60_000, message: 'işler bitmedi' }).catch(() => undefined);
    await sleep(4000);

    const startsPerJob = Object.fromEntries(jobs.map((j) => [j.id, starts.filter((s) => s.job === j.id).length]));
    const expected = Object.fromEntries(jobs.map((j) => [j.id, 1]));
    expect(startsPerJob, `başlangıçlar: ${starts.map((s) => s.job).join(', ')}`).toEqual(expected);
  });
});
