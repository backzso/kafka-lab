import type { RetryPipeline, RetryPipelineOptions } from './types.js';

/**
 * Ana topic'i tüketir. Başarısız mesajlar ana akışı bloklamadan retry topic üzerinden
 * `retryDelayMs` aralıklarla tekrar denenir; `maxAttempts` sonunda ya da NonRetriableError'da DLQ'ya gider.
 */
export async function startRetryPipeline(opts: RetryPipelineOptions): Promise<RetryPipeline> {
  const consumer = opts.kafka.consumer({ groupId: opts.groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topics.main, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value!.toString();
      // TODO: Bu yaklaşımın sorunları neler? (backoff yok, bloklar, sonunda mesajı çöpe atar)
      for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
        try {
          await opts.handler(value, attempt);
          return;
        } catch {
          // tekrar dene
        }
      }
      console.warn('mesaj işlenemedi, atlanıyor', value);
    },
  });
  return { stop: () => consumer.disconnect() };
}
