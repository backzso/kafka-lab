import { logLevel as KafkaLogLevel } from 'kafkajs';

try {
  process.loadEnvFile?.('.env');
} catch {
  // .env opsiyonel
}

export const config = {
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092,localhost:19093,localhost:19094').split(','),
  pgUrl: process.env.PG_URL ?? 'postgres://lab:lab@localhost:15432/kafka_lab',
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:16379',
  kafkaLogLevel:
    KafkaLogLevel[(process.env.KAFKA_LOG_LEVEL ?? 'NOTHING') as keyof typeof KafkaLogLevel] ??
    KafkaLogLevel.NOTHING,
  topicPrefix: 'lab.',
  composeProject: 'kafka-lab',
};

/** Broker node id -> docker compose servis adı */
export const brokerServices: Record<number, string> = {
  1: 'kafka-1',
  2: 'kafka-2',
  3: 'kafka-3',
};
