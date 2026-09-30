import type { Consumer, ConsumerConfig, Kafka } from 'kafkajs';

export interface Job {
  id: string;
  durationMs: number;
}

export interface JobWorkerOptions {
  topic: string;
  /** groupId, sessionTimeout, heartbeatInterval: aynen kafka.consumer(...)'a ver. */
  consumerConfig: ConsumerConfig;
  /** Uzun süren iş (ör. rapor üretimi). */
  runJob: (job: Job) => Promise<void>;
}

export async function startJobWorker(kafka: Kafka, opts: JobWorkerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const job: Job = JSON.parse(message.value!.toString());
      // TODO: İş sessionTimeout'tan uzun sürüyor.
      await opts.runJob(job);
    },
  });
  return consumer;
}
