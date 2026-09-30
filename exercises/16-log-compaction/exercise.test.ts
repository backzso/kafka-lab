import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Kafka, Producer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { headersToRecord, type ReadMessage } from '../../src/lib/messages.js';
import { sleep, waitFor } from '../../src/lib/wait.js';
import type { UserProfile } from './types.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

/** Topic'teki fiziksel kayıtları (tombstone'lar dahil) high watermark'a kadar okur. */
async function readRaw(kafka: Kafka, topic: string): Promise<ReadMessage[]> {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic).finally(() => admin.disconnect());
  const remaining = new Map(offsets.filter((p) => BigInt(p.high) > BigInt(p.low)).map((p) => [p.partition, BigInt(p.high)]));
  const out: ReadMessage[] = [];
  if (!remaining.size) return out;

  const consumer = kafka.consumer({ groupId: uniqueName('raw'), maxWaitTimeInMs: 200 });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      out.push({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        value: message.value?.toString() ?? null,
        headers: headersToRecord(message.headers),
      });
      if (BigInt(message.offset) + 1n >= (remaining.get(partition) ?? 0n)) remaining.delete(partition);
    },
  });
  try {
    await waitFor(() => remaining.size === 0, { timeoutMs: 20_000, message: 'ham okuma bitmedi' });
  } finally {
    await consumer.disconnect();
  }
  return out;
}

const profile = (userId: string, version: number): UserProfile => ({ userId, email: `${userId}+v${version}@example.com`, version });

describe('16 - log compaction', () => {
  const kafka = createKafka();
  let producer: Producer;
  let topic: string;
  let written = 0;
  let compacted = false;
  let raw: ReadMessage[] = [];

  beforeAll(async () => {
    // 4 partition / 3 kullanıcı: en az bir partition boş kalır.
    topic = await createTopic(kafka, { label: 'user-profiles', partitions: 4, configs: impl.compactedTopicConfigs() });
    producer = kafka.producer({ idempotent: true });
    await producer.connect();

    // Faz 1: her kullanıcı 30 kez güncellenir, carol silinir.
    for (let v = 1; v <= 30; v++) {
      for (const u of ['alice', 'bob', 'carol']) {
        await impl.upsertProfile(producer, topic, profile(u, v));
        written++;
      }
    }
    await impl.deleteProfile(producer, topic, 'carol');
    written++;

    // Faz 2: segment.ms dolsun; yeni yazmalar segment'i kapatır, faz 1 compaction'a açılır.
    await sleep(2000);
    await impl.upsertProfile(producer, topic, profile('alice', 31));
    await impl.upsertProfile(producer, topic, profile('bob', 31));
    // Silme talebi tekrar geldi (idempotent olmalı).
    await impl.deleteProfile(producer, topic, 'carol');
    written += 3;

    // Log cleaner broker'da arka planda çalışır (log.cleaner.backoff.ms=15sn).
    compacted = await waitFor(async () => (await readRaw(kafka, topic)).length <= 6, {
      timeoutMs: 60_000,
      intervalMs: 3000,
    }).then(
      () => true,
      () => false,
    );
    raw = await readRaw(kafka, topic);
  });

  afterAll(async () => {
    await producer?.disconnect();
  });

  it('compaction çalıştı: key başına en fazla iki kayıt kaldı', () => {
    const perKey = Object.fromEntries(['alice', 'bob', 'carol'].map((u) => [u, raw.filter((m) => m.key === u).length]));
    expect(compacted, `60 sn sonra topic’te hâlâ ${raw.length}/${written} kayıt var: ${JSON.stringify(perKey)}`).toBe(true);
    for (const [user, count] of Object.entries(perKey)) expect(count, `${user} kayıt sayısı`).toBeLessThanOrEqual(2);
  });

  it('silme tombstone ile yapıldı: carol’ın son kaydının value’su null', () => {
    const last = raw.filter((m) => m.key === 'carol').at(-1);
    expect(last, 'carol için hiç kayıt yok (tombstone da silinmiş olabilir, ama bu kadar hızlı olmamalı)').toBeDefined();
    expect(last!.value, 'silme bir tombstone (value: null) olmalı; bu kayıt compaction’dan sonra da topic’te kalır').toBeNull();
    expect(raw.filter((m) => m.key !== 'carol' && m.value === null), 'silinmeyen kullanıcılarda tombstone').toEqual([]);
  });

  it('snapshot son durumu verir ve offset boşluklarına rağmen biter', async () => {
    const started = Date.now();
    const snapshot = await Promise.race([impl.loadSnapshot(kafka, topic), sleep(15_000).then(() => 'timeout' as const)]);
    expect(snapshot, 'loadSnapshot 15 sn içinde bitmedi. Compaction sonrası offset’ler ardışık değil').not.toBe('timeout');
    expect(Object.fromEntries(snapshot as Map<string, UserProfile>), 'snapshot: her kullanıcının son profili, silinenler yok').toEqual({ alice: profile('alice', 31), bob: profile('bob', 31) });
    expect(Date.now() - started, 'snapshot gereğinden uzun sürdü (sessizlik beklemek yerine nerede biteceğini bil)').toBeLessThan(4000);
  });

  it('boş topic’te snapshot hemen döner', async () => {
    const empty = await createTopic(kafka, { label: 'user-profiles-empty', partitions: 3, configs: impl.compactedTopicConfigs() });
    const started = Date.now();
    const snapshot = await impl.loadSnapshot(kafka, empty);
    expect(snapshot.size).toBe(0);
    expect(Date.now() - started, 'boş topic için bekleme yapılmamalı').toBeLessThan(2000);
  });
});
