import { afterEach, describe, expect, it } from 'vitest';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { produce } from '../../src/lib/messages.js';
import { spawnWorker, type WorkerHandle, type WorkerMessage } from '../../src/lib/worker.js';
import { sleep, waitFor } from '../../src/lib/wait.js';

const workerUrl = new URL('./worker.ts', import.meta.url);
const SESSION_TIMEOUT = 15_000;
const kafka = createKafka();

/** Grup `members` üyeyle Stable olana kadar bekler. */
async function waitForGroup(groupId: string, members: number) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await waitFor(async () => {
      const { groups } = await admin.describeGroups([groupId]);
      return groups[0]?.state === 'Stable' && groups[0].members.length === members;
    }, { timeoutMs: 30_000, intervalMs: 300, message: `grup ${members} üyeyle stabil olmadı` });
  } finally {
    await admin.disconnect();
  }
}

const ofType = (messages: WorkerMessage[], type: string) => messages.filter((m) => m.type === type);

/** İki pod'lu bir servis kurar; A mesaj işlerken hazır döner. */
async function twoPods(workers: WorkerHandle[]) {
  const topic = await createTopic(kafka, { label: 'orders', partitions: 4 });
  const group = uniqueName('order-service');
  const env = { TOPIC: topic, GROUP: group };

  const a = spawnWorker(workerUrl, { ...env, NAME: 'pod-a' });
  workers.push(a);
  await waitForGroup(group, 1);
  const b = spawnWorker(workerUrl, { ...env, NAME: 'pod-b' });
  workers.push(b);
  await waitForGroup(group, 2);

  const values = Array.from({ length: 40 }, (_, i) => `order-${i}`);
  await produce(kafka, topic, values.map((value, i) => ({ partition: i % 4, value })));
  await waitFor(() => ofType(a.messages, 'done').length >= 3, { timeoutMs: 20_000, message: 'pod-a mesaj işlemeye başlamadı' });
  return { a, b, values, group };
}

/** Coordinator'ın pod-a'yı gruptan çıkarıp partition'ları pod-b'ye verdiği (grup 1 üyeyle Stable) ana kadar geçen süre. */
async function takeoverMs(group: string, from: number, timeoutMs: number) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await waitFor(async () => {
      const { groups } = await admin.describeGroups([group]);
      return groups[0]?.state === 'Stable' && groups[0].members.length === 1;
    }, { timeoutMs, intervalMs: 100, message: 'pod-a gruptan çıkarılmadı' });
  } finally {
    await admin.disconnect();
  }
  return Date.now() - from;
}

describe('17 - graceful shutdown ve rebalance', () => {
  const workers: WorkerHandle[] = [];
  afterEach(() => workers.splice(0).forEach((w) => w.kill()));

  it('rolling deploy: SIGTERM alan pod elindeki işi bitirir, gruptan hemen ayrılır; kayıp ve tekrar yok', async () => {
    const { a, b, values, group } = await twoPods(workers);

    const sigtermAt = Date.now();
    a.kill('SIGTERM');
    const takeover = takeoverMs(group, sigtermAt, SESSION_TIMEOUT + 10_000);
    const exit = await Promise.race([a.exited, sleep(10_000).then(() => null)]);
    expect(exit, 'pod-a SIGTERM sonrası 10 sn içinde kapanmadı').not.toBeNull();
    expect(exit, 'pod-a sinyalle öldü ya da hata koduyla çıktı; SIGTERM yakalanıp temiz kapanılmalı').toEqual({ code: 0, signal: null });

    const aStarted = ofType(a.messages, 'start').map((m) => m.value);
    const aDone = ofType(a.messages, 'done').map((m) => m.value);
    expect(aStarted.filter((v) => !aDone.includes(v)), 'pod-a’nın yarıda bıraktığı işler').toEqual([]);

    const takeoverAfter = await takeover;
    console.log(`[17] graceful: pod-b ${takeoverAfter}ms sonra devraldı (sessionTimeout ${SESSION_TIMEOUT}ms)`);
    expect(takeoverAfter, 'devralma sessionTimeout’u bekledi: pod-a gruptan ayrıldığını (LeaveGroup) bildirmedi').toBeLessThan(5000);

    await waitFor(() => new Set([...aDone, ...ofType(b.messages, 'done').map((m) => m.value)]).size === values.length, {
      timeoutMs: 30_000,
      message: 'tüm mesajlar işlenmedi',
    });
    await sleep(1000);
    const done = [...aDone, ...ofType(b.messages, 'done').map((m) => m.value as string)];
    const duplicates = values.filter((v) => done.filter((d) => d === v).length > 1);
    expect(duplicates, 'tekrar işlenen mesajlar: pod-a bitirdiği işlerin offset’ini commit etmeden çıktı').toEqual([]);
  });

  it('takılan bir iş kapanışı sonsuza kadar geciktirmez', async () => {
    const topic = await createTopic(kafka, { label: 'orders' });
    const group = uniqueName('order-service');
    const w = spawnWorker(workerUrl, { TOPIC: topic, GROUP: group, NAME: 'pod-stuck' });
    workers.push(w);
    await waitForGroup(group, 1);
    await produce(kafka, topic, [{ value: 'stuck' }]);
    await w.waitForMessage((m) => m.type === 'start' && m.value === 'stuck', 20_000);

    const sigtermAt = Date.now();
    w.kill('SIGTERM');
    const exit = await Promise.race([w.exited, sleep(8000).then(() => null)]);
    const elapsed = Date.now() - sigtermAt;
    expect(exit, 'process 8 sn sonra hâlâ ayakta: shutdownTimeoutMs (3 sn) uygulanmadı, Kubernetes SIGKILL atacak').not.toBeNull();
    expect(exit!.signal, 'process sinyalle öldü: SIGTERM yakalanmadı').toBeNull();
    expect(exit!.code, 'zorla kapanış başarılı (0) gibi görünmemeli').not.toBe(0);
    expect(elapsed, 'süre sınırı çok erken devreye girdi').toBeGreaterThanOrEqual(2500);
  });

  it('bilgi: crash (SIGKILL) sonrası partition’lar sessionTimeout dolana kadar sahipsiz kalır (her zaman geçer)', async () => {
    const { a, group } = await twoPods(workers);

    const killedAt = Date.now();
    a.kill('SIGKILL');
    const takeover = await takeoverMs(group, killedAt, SESSION_TIMEOUT + 15_000);
    console.log(`[17] crash: pod-b ${takeover}ms sonra devraldı (sessionTimeout ${SESSION_TIMEOUT}ms)`);
    // Coordinator ölü üyeyi sadece heartbeat kesilince fark eder. Static membership bu bekleyişi restart için kullanılabilir kılar.
    expect(takeover).toBeGreaterThan(SESSION_TIMEOUT - 3000);
  });
});
