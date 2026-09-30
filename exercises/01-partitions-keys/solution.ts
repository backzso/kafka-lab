import type { Consumer, Kafka, Producer } from 'kafkajs';
import type { CustomerEvent } from './types.js';

export async function publishCustomerEvents(producer: Producer, topic: string, events: CustomerEvent[]): Promise<void> {
  await producer.send({
    topic,
    // Sıralama birimi müşteri: customerId aynı partition'ı garanti eder.
    messages: events.map((e) => ({ key: e.customerId, value: JSON.stringify(e) })),
  });
}

export async function startCustomerConsumer(
  kafka: Kafka,
  topic: string,
  groupId: string,
  handler: (event: CustomerEvent, partition: number) => Promise<void>,
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    partitionsConsumedConcurrently: 6,
    eachMessage: async ({ partition, message }) => {
      await handler(JSON.parse(message.value!.toString()), partition);
    },
  });
  return consumer;
}
