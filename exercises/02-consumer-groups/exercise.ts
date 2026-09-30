import type { Consumer, Kafka, KafkaMessage } from 'kafkajs';

export interface WorkerOptions {
  topic: string;
  /** Servis adı, ör. "billing-service" */
  serviceName: string;
  /** Instance adı, ör. pod adı "billing-service-7f9c-abcde" */
  instanceId: string;
  onMessage: (message: KafkaMessage, partition: number) => Promise<void>;
}

/** Bir servis instance'ı başlatır. Aynı servisin instance'ları işi paylaşmalı. */
export async function startWorker(kafka: Kafka, opts: WorkerOptions): Promise<Consumer> {
  // TODO: groupId seçimi doğru mu?
  const consumer = kafka.consumer({ groupId: `${opts.serviceName}-${opts.instanceId}` });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message, partition }) => opts.onMessage(message, partition) });
  return consumer;
}

/** Topic'te hâlihazırda bulunan tüm mesajları (ve yenilerini) okuyan consumer başlatır. */
export async function replayAll(
  kafka: Kafka,
  topic: string,
  groupId: string,
  onMessage: (message: KafkaMessage) => Promise<void>,
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  // TODO: Yeni bir grup nereden başlar?
  await consumer.subscribe({ topic });
  await consumer.run({ eachMessage: async ({ message }) => onMessage(message) });
  return consumer;
}

/** Grubun `topic` üzerindeki offset'lerini `timestamp` anına geri sarar. Grup aktifse hata fırlatır. */
export async function rewindGroupToTimestamp(
  kafka: Kafka,
  groupId: string,
  topic: string,
  timestamp: number,
): Promise<void> {
  // TODO
  throw new Error('TODO: rewindGroupToTimestamp');
}
