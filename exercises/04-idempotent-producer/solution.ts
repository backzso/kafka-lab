import type { Kafka, Producer } from 'kafkajs';

export function createPaymentProducer(kafka: Kafka): Producer {
  // PID + sequence ile broker retry'ları deduplicate eder. acks=-1 zorunlu (send'de varsayılan zaten -1).
  return kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
}
