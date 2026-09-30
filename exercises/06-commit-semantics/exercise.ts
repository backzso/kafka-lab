import type { Consumer, ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';

export interface RunOptions {
  kafka: Kafka;
  topic: string;
  /** groupId ve timeout ayarları; kafka.consumer(...)'a aynen ver. */
  consumerConfig: ConsumerConfig;
  handle: (message: KafkaMessage) => Promise<void>;
}

/** Kayıp yok; crash'te en fazla işlenmekte olan mesaj tekrar gelir. */
export async function runAtLeastOnce(opts: RunOptions): Promise<Consumer> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  // TODO: Offset'ler ne zaman commit ediliyor?
  await consumer.run({
    eachMessage: async ({ message }) => {
      await opts.handle(message);
    },
  });
  return consumer;
}

/** Hiçbir mesaj iki kez işlenmez; crash'te işlenmekte olan mesaj kaybolabilir. */
export async function runAtMostOnce(opts: RunOptions): Promise<Consumer> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  // TODO
  await consumer.run({
    eachMessage: async ({ message }) => {
      await opts.handle(message);
    },
  });
  return consumer;
}
