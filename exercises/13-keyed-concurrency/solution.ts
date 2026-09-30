import type { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import type { KeyedConsumerOptions } from './types.js';

/** Basit semaphore: aynı anda en fazla `limit` iş. */
function createLimiter(limit: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  return async <T>(fn: () => Promise<T>): Promise<T> => {
    if (active >= limit) await new Promise<void>((r) => queue.push(r));
    active++;
    try {
      return await fn();
    } finally {
      active--;
      queue.shift()?.();
    }
  };
}

export async function startKeyedConsumer(kafka: Kafka, opts: KeyedConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  const limit = createLimiter(opts.maxConcurrency);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    // Offset'leri biz resolve ediyoruz: sadece kesintisiz tamamlanmış önek.
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const byKey = new Map<string, KafkaMessage[]>();
      for (const m of batch.messages) {
        const key = m.key?.toString() ?? '';
        byKey.set(key, [...(byKey.get(key) ?? []), m]);
      }

      const done = new Set<string>();
      let failure: unknown;
      await Promise.all(
        [...byKey].map(async ([key, messages]) => {
          for (const m of messages) {
            // Bir hata olduysa bu key'de devam etme: sonraki mesaj başarısız olandan önce işlenmemeli.
            if (failure) return;
            try {
              await limit(() => opts.handle(key, m.value!.toString()));
              done.add(m.offset);
            } catch (e) {
              failure ??= e;
              return;
            }
          }
        }),
      );

      // Commit edilen offset "öncesindeki her şey bitti" demek: ilk bitmemiş mesajda dur.
      for (const m of batch.messages) {
        if (!done.has(m.offset)) break;
        resolveOffset(m.offset);
      }
      await heartbeat();
      // Batch yeniden denenirken sadece çözülmemiş kısımdan itibaren tekrar gelir; önekteki mesajlar tekrar işlenmez.
      if (failure) throw failure;
    },
  });
  return consumer;
}
