import { afterEach, describe, expect, it } from 'vitest';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopics, uniqueName } from '../../src/lib/topics.js';
import { produce, readAll } from '../../src/lib/messages.js';
import { spawnWorker, type WorkerHandle } from '../../src/lib/worker.js';
import { sleep } from '../../src/lib/wait.js';

const workerUrl = new URL('./worker.ts', import.meta.url);
const inputs = Array.from({ length: 8 }, (_, i) => `event-${i}`);
const expectedOutputs = inputs.map((v) => v.toUpperCase());

const countBy = (values: (string | null)[]) =>
  Object.fromEntries(expectedOutputs.map((v) => [v, values.filter((x) => x === v).length]));
const once = Object.fromEntries(expectedOutputs.map((v) => [v, 1]));

describe('12 - Kafka transaction ve exactly-once', () => {
  const kafka = createKafka();
  const workers: WorkerHandle[] = [];
  afterEach(() => workers.splice(0).forEach((w) => w.kill()));

  async function setup() {
    const [input, output] = await createTopics(kafka, [{ label: 'eos-in' }, { label: 'eos-out' }]);
    await produce(kafka, input, inputs.map((value) => ({ key: value, value })));
    const env = { INPUT: input, OUTPUT: output, GROUP: uniqueName('eos'), TXN_ID: uniqueName('eos-processor-0') };
    return { output, env };
  }

  const waitLast = (w: WorkerHandle) =>
    w.waitForMessage((m) => m.type === 'beforeCommit' && m.value === inputs.at(-1), 60_000);

  it('commit’ten hemen önce crash: read_committed downstream her olayı tam bir kez görür', async () => {
    const { output, env } = await setup();

    const first = spawnWorker(workerUrl, { ...env, FAULT: 'crash', FAULT_AT: 'event-3' });
    workers.push(first);
    const exit = await Promise.race([first.exited, sleep(40_000).then(() => null)]);
    expect(exit?.signal, 'ilk worker event-3’te çökmeliydi').toBe('SIGKILL');

    const second = spawnWorker(workerUrl, { ...env, FAULT: '' });
    workers.push(second);
    await waitLast(second);
    await sleep(1000);

    const committed = (await readAll(kafka, output, { expected: inputs.length, timeoutMs: 20_000 })).map((m) => m.value);
    const uncommitted = (await readAll(kafka, output, { readUncommitted: true, timeoutMs: 4000 })).map((m) => m.value);
    console.log(`[12] read_committed: ${committed.length} kayıt, read_uncommitted: ${uncommitted.length} kayıt (abort edilenler dahil)`);

    expect(countBy(committed)).toEqual(once);
  });

  it('zombie instance (donmuş, sonra uyanmış) fence edilir ve duplicate üretmez', async () => {
    const { output, env } = await setup();

    const zombie = spawnWorker(workerUrl, { ...env, FAULT: 'freeze', FAULT_AT: 'event-2' });
    workers.push(zombie);
    await zombie.waitForMessage((m) => m.type === 'freezing', 40_000);

    // Donmuş instance session timeout ile gruptan düşer; yenisi partition'ı ve transactionalId'yi devralır.
    const successor = spawnWorker(workerUrl, { ...env, FAULT: '' });
    workers.push(successor);
    await waitLast(successor);
    await sleep(1000);

    zombie.child.kill('SIGCONT');
    await zombie.waitForMessage((m) => m.type === 'resumed', 10_000);
    await sleep(5000);

    const committed = (await readAll(kafka, output, { expected: inputs.length, timeoutMs: 20_000 })).map((m) => m.value);
    expect(countBy(committed), 'zombie’nin yazdıkları downstream’e sızmamalı').toEqual(once);
    expect(zombie.messages.some((m) => m.type === 'fenced'), 'zombie fence edildiğini fark edip durmalı (onFenced)').toBe(true);
  });
});
