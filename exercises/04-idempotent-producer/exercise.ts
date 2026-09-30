import type { Kafka, Producer } from 'kafkajs';

/** Ödeme olaylarını yazacak producer. Ağ hatası kaynaklı retry'lar duplicate üretmemeli. */
export function createPaymentProducer(kafka: Kafka): Producer {
  // TODO
  return kafka.producer();
}
