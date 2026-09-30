import type { Message, Producer } from 'kafkajs';

/**
 * Mesajları gönderir. Promise resolve olduysa mesajlar broker kaybetse bile kaybolmamalı.
 */
export async function durableSend(producer: Producer, topic: string, messages: Message[]): Promise<void> {
  // TODO: Bu gönderim neyi garanti ediyor?
  await producer.send({ topic, messages, acks: 1 });
}

export interface FailureInfo {
  /** Kafka protokol hata tipi, ör. "NOT_ENOUGH_REPLICAS". Bulunamazsa "UNKNOWN". */
  type: string;
  /** Asıl hata retriable mı? */
  retriable: boolean;
}

/** producer.send'in fırlattığı hatanın asıl sebebini döndürür. */
export function describeFailure(error: unknown): FailureInfo {
  // TODO: Dıştaki hata gerçekten asıl sebep mi?
  throw new Error('TODO: describeFailure');
}
