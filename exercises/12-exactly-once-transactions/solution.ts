import type { Processor, ProcessorOptions } from './types.js';

const FENCED = new Set(['PRODUCER_FENCED', 'INVALID_PRODUCER_EPOCH', 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED']);

function isFenced(error: unknown): boolean {
  for (let e = error as { type?: string; cause?: unknown } | undefined, i = 0; e && i < 10; i++) {
    if (e.type && FENCED.has(e.type)) return true;
    e = e.cause as typeof e;
  }
  return false;
}

export async function startProcessor(opts: ProcessorOptions): Promise<Processor> {
  const { kafka, inputTopic, outputTopic, transform, faults = {} } = opts;
  const groupId = opts.consumerConfig.groupId;
  // Kalıcı transactionalId: yeni instance initTransactions yaptığında epoch artar,
  // eski instance'ın açık transaction'ı abort edilir ve eski producer fence olur.
  const producer = kafka.producer({ transactionalId: opts.transactionalId, idempotent: true, maxInFlightRequests: 1 });
  // read_committed (kafkajs varsayılanı): upstream'in abort edilmiş kayıtlarını görme.
  const consumer = kafka.consumer({ ...opts.consumerConfig, readUncommitted: false });
  let fenced = false;

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: inputTopic, fromBeginning: true });
  await consumer.run({
    // Offset'ler transaction'ın parçası olarak commit edilir; consumer ayrıca commit etmemeli.
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      if (fenced) return;
      const txn = await producer.transaction();
      try {
        await txn.send({
          topic: outputTopic,
          messages: [{ key: message.key, value: transform(message.value!.toString()) }],
        });
        await txn.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic, partitions: [{ partition, offset: (BigInt(message.offset) + 1n).toString() }] }],
        });
        await faults.beforeCommit?.(message);
        await txn.commit();
      } catch (e) {
        if (isFenced(e)) {
          // Yerimize başka bir instance geldi: yazdıklarımız zaten abort edildi. Retry yok, dur.
          fenced = true;
          opts.onFenced?.(e as Error);
          void consumer.disconnect().catch(() => undefined);
          return;
        }
        await txn.abort().catch(() => undefined);
        throw e;
      }
    },
  });
  return {
    async stop() {
      await consumer.disconnect().catch(() => undefined);
      await producer.disconnect().catch(() => undefined);
    },
  };
}
