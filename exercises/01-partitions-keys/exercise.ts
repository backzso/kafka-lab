import type { Consumer, Kafka, Producer } from 'kafkajs';
import type { CustomerEvent } from './types.js';

/**
 * Olayları topic'e yaz. Aynı müşterinin olayları sırayla işlenebilmeli.
 */
export async function publishCustomerEvents(producer: Producer, topic: string, events: CustomerEvent[]): Promise<void> {
  // TODO: Hangi alan key olmalı? Key olmadan mesajlar partition'lara dağılır.
  await producer.send({
    topic,
    messages: events.map((e) => ({ value: JSON.stringify(e) })),
  });
}

/**
 * Topic'i tüket; her mesaj için handler'ı çağır. Dönen consumer'ı test kapatır.
 */
export async function startCustomerConsumer(
  kafka: Kafka,
  topic: string,
  groupId: string,
  handler: (event: CustomerEvent, partition: number) => Promise<void>,
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  // TODO: Partition'ları paralel işle.
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      await handler(JSON.parse(message.value!.toString()), partition);
    },
  });
  return consumer;
}
