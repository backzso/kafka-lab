import type { Kafka, Producer } from 'kafkajs';
import { uniqueName } from '../../src/lib/topics.js';
import type { UserProfile } from './types.js';

export function compactedTopicConfigs(): Record<string, string> {
  return {
    // Key başına son kayıt kalır; süre/boyuta göre silme yok.
    'cleanup.policy': 'compact',
    // Aktif segment asla compact edilmez. Lab'de segment'lerin hızlı kapanması için; prod'da varsayılan 7 gün.
    'segment.ms': '500',
    // Log'un ne kadarı "kirli" (tekrar eden key) olunca temizleneceği. Varsayılan 0.5.
    'min.cleanable.dirty.ratio': '0.01',
  };
}

export async function upsertProfile(producer: Producer, topic: string, profile: UserProfile): Promise<void> {
  await producer.send({ topic, messages: [{ key: profile.userId, value: JSON.stringify(profile) }] });
}

export async function deleteProfile(producer: Producer, topic: string, userId: string): Promise<void> {
  // Tombstone: value null. Compaction key'in eski kayıtlarını siler, tombstone'u delete.retention.ms sonra siler.
  await producer.send({ topic, messages: [{ key: userId, value: null }] });
}

export async function loadSnapshot(kafka: Kafka, topic: string): Promise<Map<string, UserProfile>> {
  const state = new Map<string, UserProfile>();

  // Bitiş koşulu: çağrı anındaki high watermark. Mesaj saymak işe yaramaz: compaction offset'lerde boşluk bırakır.
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic).finally(() => admin.disconnect());
  const remaining = new Map<number, bigint>();
  for (const p of offsets) {
    if (BigInt(p.high) > BigInt(p.low)) remaining.set(p.partition, BigInt(p.high));
  }
  // Boş partition'dan mesaj gelmeyecek; hepsi boşsa consumer açmaya gerek yok.
  if (remaining.size === 0) return state;

  let caughtUp!: () => void;
  const done = new Promise<void>((resolve) => (caughtUp = resolve));
  const consumer = kafka.consumer({ groupId: uniqueName('snapshot'), maxWaitTimeInMs: 200 });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    // Snapshot okuyucusu grubun ilerlemesini kaydetmez: her açılışta baştan okunur.
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      const key = message.key!.toString();
      if (message.value === null) state.delete(key);
      else state.set(key, JSON.parse(message.value.toString()));

      const end = remaining.get(partition);
      if (end !== undefined && BigInt(message.offset) + 1n >= end) {
        remaining.delete(partition);
        if (remaining.size === 0) caughtUp();
      }
    },
  });
  try {
    await done;
  } finally {
    await consumer.disconnect();
  }
  return state;
}
