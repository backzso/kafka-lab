import type { ConsumerConfig } from 'kafkajs';

export interface KeyedConsumerOptions {
  topic: string;
  consumerConfig: ConsumerConfig;
  /** Aynı anda en fazla bu kadar handler çalışabilir. */
  maxConcurrency: number;
  handle: (key: string, value: string) => Promise<void>;
}
