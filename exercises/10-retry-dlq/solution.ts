import type { KafkaMessage } from 'kafkajs';
import { sleep } from '../../src/lib/wait.js';
import { NonRetriableError, type RetryPipeline, type RetryPipelineOptions } from './types.js';

export async function startRetryPipeline(opts: RetryPipelineOptions): Promise<RetryPipeline> {
  const { kafka, topics, maxAttempts, retryDelayMs, handler } = opts;
  const producer = kafka.producer({ idempotent: true });
  const main = kafka.consumer({ groupId: opts.groupId });
  // Retry topic'i ayrı bir grup tüketir: ana akış retry beklemelerinden etkilenmez.
  const retry = kafka.consumer({ groupId: `${opts.groupId}.retry` });

  /** Başarısız denemeyi yönlendirir. Yönlendirme yazılmadan offset commit edilmez (at-least-once). */
  async function routeFailure(message: KafkaMessage, attempt: number, error: unknown) {
    const reason = error instanceof Error ? error.message : String(error);
    if (error instanceof NonRetriableError || attempt >= maxAttempts) {
      await producer.send({
        topic: topics.dlq,
        messages: [{
          key: message.key,
          value: message.value,
          headers: { ...message.headers, attempts: String(attempt), error: reason, 'original-topic': topics.main },
        }],
      });
      return;
    }
    await producer.send({
      topic: topics.retry,
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          ...message.headers,
          attempt: String(attempt + 1),
          'retry-at': String(Date.now() + retryDelayMs),
          error: reason,
        },
      }],
    });
  }

  await producer.connect();
  await Promise.all([main.connect(), retry.connect()]);
  await main.subscribe({ topic: topics.main, fromBeginning: true });
  await retry.subscribe({ topic: topics.retry, fromBeginning: true });

  await main.run({
    eachMessage: async ({ message }) => {
      try {
        await handler(message.value!.toString(), 1);
      } catch (e) {
        await routeFailure(message, 1, e);
      }
    },
  });

  await retry.run({
    eachMessage: async ({ message, heartbeat }) => {
      const attempt = Number(message.headers?.attempt?.toString() ?? '2');
      const retryAt = Number(message.headers?.['retry-at']?.toString() ?? '0');
      // Tüm mesajlar aynı gecikmeyle yazıldığı için retry topic retry-at'e göre sıralı: baştakini beklemek yeterli.
      while (Date.now() < retryAt) {
        await sleep(Math.min(retryAt - Date.now(), 500));
        await heartbeat();
      }
      try {
        await handler(message.value!.toString(), attempt);
      } catch (e) {
        await routeFailure(message, attempt, e);
      }
    },
  });

  return {
    async stop() {
      await Promise.all([main.disconnect(), retry.disconnect()]);
      await producer.disconnect();
    },
  };
}
