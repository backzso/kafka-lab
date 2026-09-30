import type { Message, Producer } from 'kafkajs';

export async function durableSend(producer: Producer, topic: string, messages: Message[]): Promise<void> {
  // acks=-1: ISR'daki tüm replikalar yazmadan onay yok. min.insync.replicas=2 ile en az 2 kopya.
  await producer.send({ topic, messages, acks: -1 });
}

export interface FailureInfo {
  type: string;
  retriable: boolean;
}

export function describeFailure(error: unknown): FailureInfo {
  // Wrapper'ları (KafkaJSNumberOfRetriesExceeded vb.) soyarak protokol hatasına in.
  let current: unknown = error;
  for (let depth = 0; current && depth < 10; depth++) {
    const e = current as { type?: string; retriable?: boolean; cause?: unknown };
    if (typeof e.type === 'string') return { type: e.type, retriable: e.retriable === true };
    current = e.cause;
  }
  return { type: 'UNKNOWN', retriable: false };
}
