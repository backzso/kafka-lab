import type { ConsumerConfig } from 'kafkajs';

/** Downstream geçici olarak kullanılamıyor (ör. HTTP 503/429). `retryAfterMs` sonra tekrar denenebilir. */
export class DownstreamUnavailableError extends Error {
  constructor(public readonly retryAfterMs: number) {
    super(`downstream unavailable, retry after ${retryAfterMs}ms`);
    this.name = 'DownstreamUnavailableError';
  }
}

export interface ResilientConsumerOptions {
  topic: string;
  consumerConfig: ConsumerConfig;
  /** Mesajı downstream'e yazar. DownstreamUnavailableError fırlatabilir. */
  sink: (value: string) => Promise<void>;
}
