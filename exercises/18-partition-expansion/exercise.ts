import type { Consumer, Kafka, Producer } from 'kafkajs';

export interface AccountConsumerOptions {
  topic: string;
  groupId: string;
  handle: (accountId: string, value: string, partition: number) => Promise<void>;
}

/** Hesap olaylarını tüketir. Topic'in partition sayısı çalışırken artırılabilir. */
export async function startAccountConsumer(kafka: Kafka, opts: AccountConsumerOptions): Promise<Consumer> {
  // TODO: Topic'e sonradan eklenen partition'ları bu consumer ne zaman fark eder? Oradaki mesajları nereden okur?
  const consumer = kafka.consumer({ groupId: opts.groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      await opts.handle(message.key!.toString(), message.value!.toString(), partition);
    },
  });
  return consumer;
}

/** Hesap olaylarını hesap id'si ile key'leyerek yazar. Aynı hesabın olayları sırayla işlenmeli. */
export class AccountPublisher {
  private producer: Producer;

  constructor(
    private readonly kafka: Kafka,
    private readonly topic: string,
    /** Bu topic'i tüketen consumer group. */
    private readonly consumerGroupId: string,
  ) {
    this.producer = kafka.producer({ idempotent: true });
  }

  connect(): Promise<void> {
    return this.producer.connect();
  }

  async publish(accountId: string, value: string): Promise<void> {
    await this.producer.send({ topic: this.topic, messages: [{ key: accountId, value }] });
  }

  /**
   * Topic'in partition sayısını `count`'a çıkarır. Bu sırada `publish` eşzamanlı çağrılmaya devam edebilir:
   * hiçbir çağrı reddedilmemeli ve hiçbir hesabın sırası bozulmamalı.
   */
  async expandPartitions(count: number): Promise<void> {
    // TODO: Partition eklemek bir key'in hangi partition'a gittiğini nasıl etkiler?
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createPartitions({ topicPartitions: [{ topic: this.topic, count }] });
    } finally {
      await admin.disconnect();
    }
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }
}
