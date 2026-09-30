import type { Consumer, Kafka } from 'kafkajs';
import type { KeyedConsumerOptions } from './types.js';

/**
 * Tek partition içinde paralel işleme:
 * - aynı key'in mesajları sırayla,
 * - farklı key'ler paralel (en fazla maxConcurrency),
 * - hata olursa mesaj kaybolmaz ve key sırası bozulmaz.
 */
export async function startKeyedConsumer(kafka: Kafka, opts: KeyedConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachBatch: async ({ batch }) => {
      // TODO: "Hızlandırmak" için hepsini aynı anda işliyoruz. Neler bozuluyor?
      await Promise.all(
        batch.messages.map((m) => opts.handle(m.key!.toString(), m.value!.toString())),
      );
    },
  });
  return consumer;
}
