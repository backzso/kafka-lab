import type { Processor, ProcessorOptions } from './types.js';

/**
 * Consume → transform → produce. Crash ve zombie senaryolarında bile, read_committed okuyan
 * downstream her input için tam bir output görmeli.
 */
export async function startProcessor(opts: ProcessorOptions): Promise<Processor> {
  const { kafka, inputTopic, outputTopic, transform, faults = {} } = opts;
  // TODO: Bu producer'ın gönderdikleri ile consumer'ın commit'i arasında bir bağ yok.
  const producer = kafka.producer({ idempotent: true });
  const consumer = kafka.consumer(opts.consumerConfig);
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: inputTopic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      await producer.send({
        topic: outputTopic,
        messages: [{ key: message.key, value: transform(message.value!.toString()) }],
      });
      await faults.beforeCommit?.(message);
    },
  });
  return {
    async stop() {
      await consumer.disconnect();
      await producer.disconnect();
    },
  };
}
