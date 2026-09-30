import type { Kafka } from 'kafkajs';

/** Handler bunu fırlatırsa mesaj retry edilmeden doğrudan DLQ'ya gider (ör. şema/doğrulama hatası). */
export class NonRetriableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NonRetriableError';
  }
}

export interface RetryPipelineOptions {
  kafka: Kafka;
  topics: { main: string; retry: string; dlq: string };
  groupId: string;
  /** İlk deneme dahil toplam deneme sayısı. */
  maxAttempts: number;
  /** Başarısız denemeden sonra bir sonraki denemeye kadar en az bu kadar beklenmeli. */
  retryDelayMs: number;
  /** attempt: 1'den başlar. */
  handler: (value: string, attempt: number) => Promise<void>;
}

export interface RetryPipeline {
  stop(): Promise<void>;
}

/**
 * DLQ mesajının taşıması gereken header'lar (test bunları kontrol eder):
 * - `attempts`: toplam deneme sayısı ("3")
 * - `error`: son hatanın mesajı
 * - `original-topic`: mesajın geldiği ana topic
 * Key ve value orijinal mesajla aynı olmalı.
 */
export const DLQ_HEADERS = ['attempts', 'error', 'original-topic'] as const;
