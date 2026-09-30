import type { ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';

export interface ServiceOptions {
  kafka: Kafka;
  topic: string;
  consumerConfig: ConsumerConfig;
  shutdownTimeoutMs: number;
  handle: (message: KafkaMessage, partition: number) => Promise<void>;
}

export async function runService(opts: ServiceOptions): Promise<void> {
  const consumer = opts.kafka.consumer(opts.consumerConfig);

  let shuttingDown = false;
  const shutdown = async (signal: NodeJS.Signals) => {
    // İkinci sinyal kapanışı yeniden başlatmasın.
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[service] ${signal} alındı, kapanıyor`);

    // Kapanış takılırsa (bitmeyen iş, ulaşılamayan broker) SIGKILL'i beklemeden, hata koduyla çık.
    const deadline = setTimeout(() => {
      console.error(`[service] kapanış ${opts.shutdownTimeoutMs}ms içinde bitmedi, zorla çıkılıyor`);
      process.exit(1);
    }, opts.shutdownTimeoutMs);
    deadline.unref();

    try {
      // disconnect: yeni fetch yok -> elindeki mesajın bitmesini bekler -> offset'leri commit eder -> LeaveGroup.
      // LeaveGroup sayesinde coordinator sessionTimeout'u beklemeden rebalance başlatır.
      await consumer.disconnect();
      process.exit(0);
    } catch (e) {
      console.error('[service] kapanış hatası', e);
      process.exit(1);
    }
  };
  // Sinyal handler'ı consumer başlamadan kurulmalı: açılış sırasında gelen SIGTERM de düzgün işlenir.
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);

  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, partition }) => opts.handle(message, partition),
  });
}
