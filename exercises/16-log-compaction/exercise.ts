import type { Kafka, Producer } from 'kafkajs';
import { uniqueName } from '../../src/lib/topics.js';
import { sleep } from '../../src/lib/wait.js';
import type { UserProfile } from './types.js';

/** Profil topic'inin config'leri. Topic "her kullanıcının son profili"ni sonsuza kadar tutmalı. */
export function compactedTopicConfigs(): Record<string, string> {
  // TODO: Varsayılan cleanup.policy=delete eski kayıtları sadece süreye/boyuta göre siler.
  return {};
}

export async function upsertProfile(producer: Producer, topic: string, profile: UserProfile): Promise<void> {
  await producer.send({ topic, messages: [{ key: profile.userId, value: JSON.stringify(profile) }] });
}

/** Kullanıcıyı siler (ör. KVKK/GDPR silme talebi). Silinen kullanıcının verisi sonunda topic'ten tamamen gitmeli. */
export async function deleteProfile(producer: Producer, topic: string, userId: string): Promise<void> {
  // TODO: Bu "silindi" işareti compaction sonrasında ne olur?
  await producer.send({ topic, messages: [{ key: userId, value: JSON.stringify({ userId, deleted: true }) }] });
}

/**
 * Topic'i baştan okuyup her kullanıcının son profilini döndürür (ör. servis açılışında cache doldurma).
 * Çağrı anında topic'te olan her şeyi yansıtmalı ve sonra bitmeli.
 */
export async function loadSnapshot(kafka: Kafka, topic: string): Promise<Map<string, UserProfile>> {
  const state = new Map<string, UserProfile>();
  let lastMessageAt = Date.now();
  const consumer = kafka.consumer({ groupId: uniqueName('snapshot'), maxWaitTimeInMs: 200 });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      lastMessageAt = Date.now();
      state.set(message.key!.toString(), JSON.parse(message.value!.toString()));
    },
  });
  // TODO: "5 sn mesaj gelmezse bitti" doğru bir bitiş koşulu mu?
  while (Date.now() - lastMessageAt < 5000) await sleep(200);
  await consumer.disconnect();
  return state;
}
