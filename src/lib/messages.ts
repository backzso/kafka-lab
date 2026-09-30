import type { IHeaders, Kafka, Message } from 'kafkajs';
import { uniqueName } from './topics.js';
import { sleep } from './wait.js';

export interface ReadMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export function headersToRecord(headers: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (v === undefined) continue;
    out[k] = Array.isArray(v) ? v.map(String).join(',') : v.toString();
  }
  return out;
}

export interface ReadAllOptions {
  /** Bu kadar mesaj gelince bekleme başlar (duplicate yakalamak için settleMs kadar daha dinlenir). */
  expected?: number;
  /** expected sayısına ulaştıktan sonra ek bekleme. */
  settleMs?: number;
  timeoutMs?: number;
  /** true ise abort edilmiş transaction mesajları da okunur (read_uncommitted). */
  readUncommitted?: boolean;
}

/**
 * Topic'i baştan, geçici bir consumer group ile okur.
 * expected verilmezse timeoutMs boyunca gelen her şeyi toplar.
 */
export async function readAll(kafka: Kafka, topic: string, opts: ReadAllOptions = {}): Promise<ReadMessage[]> {
  const { expected, settleMs = 1500, timeoutMs = 20_000, readUncommitted = false } = opts;
  const consumer = kafka.consumer({
    groupId: uniqueName('reader'),
    readUncommitted,
    maxWaitTimeInMs: 200,
  });
  const out: ReadMessage[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic: t, partition, message }) => {
      out.push({
        topic: t,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        value: message.value?.toString() ?? null,
        headers: headersToRecord(message.headers),
      });
    },
  });
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (expected !== undefined && out.length >= expected) {
      await sleep(settleMs);
      break;
    }
    await sleep(100);
  }
  await consumer.disconnect();
  return out;
}

export async function produce(kafka: Kafka, topic: string, messages: Message[]): Promise<void> {
  const producer = kafka.producer({ idempotent: true });
  await producer.connect();
  try {
    await producer.send({ topic, messages, acks: -1 });
  } finally {
    await producer.disconnect();
  }
}

export const json = (v: unknown) => JSON.stringify(v);
