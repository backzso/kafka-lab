import type { Consumer, Kafka } from 'kafkajs';
import { DownstreamUnavailableError, type ResilientConsumerOptions } from './types.js';

export async function startResilientConsumer(kafka: Kafka, opts: ResilientConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, pause }) => {
      try {
        await opts.sink(message.value!.toString());
      } catch (e) {
        if (e instanceof DownstreamUnavailableError) {
          // Sadece bu partition'ı durdur. Hatayı fırlatınca offset ilerlemez ve mesaj resume'dan sonra tekrar gelir.
          const resume = pause();
          setTimeout(resume, e.retryAfterMs);
        }
        throw e;
      }
    },
  });
  return consumer;
}

export async function getConsumerLag(kafka: Kafka, groupId: string, topic: string): Promise<number> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const [committed] = await admin.fetchOffsets({ groupId, topics: [topic] });
    const latest = await admin.fetchTopicOffsets(topic);
    return latest.reduce((sum, p) => {
      const c = committed.partitions.find((x) => x.partition === p.partition)?.offset ?? '-1';
      // Commit yoksa grup en baştan (low watermark) başlayacaktır.
      const from = c === '-1' ? BigInt(p.low) : BigInt(c);
      return sum + Number(BigInt(p.high) - from);
    }, 0);
  } finally {
    await admin.disconnect();
  }
}
