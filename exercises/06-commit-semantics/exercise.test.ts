import { afterEach, describe, expect, it } from 'vitest';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { produce } from '../../src/lib/messages.js';
import { spawnWorker, type WorkerHandle } from '../../src/lib/worker.js';
import { sleep } from '../../src/lib/wait.js';

const workerUrl = new URL('./worker.ts', import.meta.url);
const values = Array.from({ length: 10 }, (_, i) => `v${i}`);

async function crashThenResume(mode: 'atLeastOnce' | 'atMostOnce', workers: WorkerHandle[]) {
  const kafka = createKafka();
  const topic = await createTopic(kafka, { label: 'commit' });
  const group = uniqueName('commit-group');
  await produce(kafka, topic, values.map((value) => ({ value })));

  const env = { MODE: mode, TOPIC: topic, GROUP: group };
  const first = spawnWorker(workerUrl, { ...env, CRASH_AT: 'v5' });
  workers.push(first);
  const exit = await Promise.race([first.exited, sleep(30_000).then(() => null)]);
  expect(exit?.signal, 'ilk worker v5’te çökmeliydi').toBe('SIGKILL');

  const second = spawnWorker(workerUrl, { ...env, CRASH_AT: '' });
  workers.push(second);
  await second.waitForMessage((m) => m.type === 'processed' && m.value === 'v9', 45_000);
  await sleep(1500);

  const processed = [...first.messages, ...second.messages].filter((m) => m.type === 'processed').map((m) => m.value as string);
  return processed;
}

const countOf = (list: string[]) => Object.fromEntries(values.map((v) => [v, list.filter((x) => x === v).length]));

describe('06 - offset commit semantiği', () => {
  const workers: WorkerHandle[] = [];
  afterEach(() => workers.splice(0).forEach((w) => w.kill()));

  it('at-least-once: kayıp yok, tamamlanmış mesajlar crash sonrası tekrar işlenmez', async () => {
    const processed = await crashThenResume('atLeastOnce', workers);
    const expected = Object.fromEntries(values.map((v) => [v, 1]));
    expect(countOf(processed), `işlenme sırası: ${processed.join(',')}`).toEqual(expected);
  });

  it('at-most-once: hiçbir mesaj iki kez işlenmez, crash anındaki mesaj kaybolur', async () => {
    const processed = await crashThenResume('atMostOnce', workers);
    const expected = Object.fromEntries(values.map((v) => [v, v === 'v5' ? 0 : 1]));
    expect(countOf(processed), `işlenme sırası: ${processed.join(',')}`).toEqual(expected);
  });
});
