import type { Consumer, ConsumerConfig, Kafka } from 'kafkajs';

export interface Job {
  id: string;
  durationMs: number;
}

export interface JobWorkerOptions {
  topic: string;
  consumerConfig: ConsumerConfig;
  runJob: (job: Job) => Promise<void>;
}

export async function startJobWorker(kafka: Kafka, opts: JobWorkerOptions): Promise<Consumer> {
  const consumer = kafka.consumer(opts.consumerConfig);
  const interval = opts.consumerConfig.heartbeatInterval ?? 1000;
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, heartbeat }) => {
      const job: Job = JSON.parse(message.value!.toString());
      const timer = setInterval(() => {
        // Rebalance sırasında heartbeat fırlatır; unhandled rejection process'i düşürmesin.
        heartbeat().catch(() => undefined);
      }, interval);
      try {
        await opts.runJob(job);
      } finally {
        clearInterval(timer);
      }
    },
  });
  return consumer;
}
