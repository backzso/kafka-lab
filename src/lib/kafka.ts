import { Kafka, type KafkaConfig } from 'kafkajs';
import { config } from './config.js';
import type { Chaos } from './chaos-socket.js';

export interface CreateKafkaOptions extends Partial<KafkaConfig> {
  /** Ağ hatası enjekte etmek için chaos soketi (bkz. chaos-socket.ts) */
  chaos?: Chaos;
}

export function createKafka(options: CreateKafkaOptions = {}): Kafka {
  const { chaos, ...rest } = options;
  return new Kafka({
    clientId: 'kafka-lab',
    brokers: config.brokers,
    logLevel: config.kafkaLogLevel,
    ...rest,
    ...(chaos ? { socketFactory: chaos.socketFactory } : {}),
  });
}
