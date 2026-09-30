import type { Consumer, Kafka } from 'kafkajs';
import type { ResilientConsumerOptions } from './types.js';

/**
 * Downstream kesintilerine dayanıklı consumer: kesinti sırasında mesaj kaybetmez, sırayı bozmaz,
 * downstream'i sürekli deneyerek yormaz (retryAfterMs'e uyar).
 */
export async function startResilientConsumer(kafka: Kafka, opts: ResilientConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        await opts.sink(message.value!.toString());
      } catch (e) {
        // TODO: Kesinti sırasında gelen mesajlara ne oluyor?
        console.warn('sink hatası, mesaj atlandı');
      }
    },
  });
  return consumer;
}

/** Grubun topic üzerindeki toplam lag'i: tüm partition'larda (son offset - commit edilen offset). */
export async function getConsumerLag(kafka: Kafka, groupId: string, topic: string): Promise<number> {
  // TODO
  throw new Error('TODO: getConsumerLag');
}
