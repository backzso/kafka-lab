import type { ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';

export interface ServiceOptions {
  kafka: Kafka;
  topic: string;
  consumerConfig: ConsumerConfig;
  /** Kapanış bu süreyi aşarsa process beklemeden çıkmalı (Kubernetes bir süre sonra SIGKILL atar). */
  shutdownTimeoutMs: number;
  handle: (message: KafkaMessage, partition: number) => Promise<void>;
}

/**
 * Consumer servisini başlatır ve process'in yaşam döngüsünü yönetir.
 * Rolling deploy'da pod önce SIGTERM alır; kapanış bitmezse SIGKILL gelir.
 */
export async function runService(opts: ServiceOptions): Promise<void> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, partition }) => opts.handle(message, partition),
  });
  // TODO: SIGTERM gelince ne olmalı?
}
