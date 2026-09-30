import type { Consumer, ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';

export interface RunOptions {
  kafka: Kafka;
  topic: string;
  consumerConfig: ConsumerConfig;
  handle: (message: KafkaMessage) => Promise<void>;
}

const next = (offset: string) => (BigInt(offset) + 1n).toString();

export async function runAtLeastOnce(opts: RunOptions): Promise<Consumer> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      await opts.handle(message);
      await consumer.commitOffsets([{ topic, partition, offset: next(message.offset) }]);
    },
  });
  return consumer;
}

export async function runAtMostOnce(opts: RunOptions): Promise<Consumer> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      await consumer.commitOffsets([{ topic, partition, offset: next(message.offset) }]);
      await opts.handle(message);
    },
  });
  return consumer;
}
