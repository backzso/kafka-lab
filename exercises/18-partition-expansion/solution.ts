import type { Admin, Consumer, Kafka, Producer } from 'kafkajs';
import { waitFor } from '../../src/lib/wait.js';

export interface AccountConsumerOptions {
  topic: string;
  groupId: string;
  handle: (accountId: string, value: string, partition: number) => Promise<void>;
}

export async function startAccountConsumer(kafka: Kafka, opts: AccountConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({
    groupId: opts.groupId,
    // Yeni partition'lar metadata yenilenince fark edilir. Varsayılan 5 dk: o süre boyunca oradaki mesajlar işlenmez.
    metadataMaxAge: 2000,
  });
  await consumer.connect();
  // Commit'i olmayan partition nereden başlar? Yeni partition'a biz fark etmeden yazılmış mesajlar olabilir:
  // "latest" ile başlarsa bunlar sessizce atlanır.
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      await opts.handle(message.key!.toString(), message.value!.toString(), partition);
    },
  });
  return consumer;
}

export class AccountPublisher {
  private producer: Producer;
  /** Genişletme sürerken dolu: yeni publish'ler bunu bekler. */
  private gate: Promise<void> | null = null;
  private readonly inFlight = new Set<Promise<unknown>>();

  constructor(
    private readonly kafka: Kafka,
    private readonly topic: string,
    private readonly consumerGroupId: string,
  ) {
    this.producer = kafka.producer({ idempotent: true });
  }

  connect(): Promise<void> {
    return this.producer.connect();
  }

  async publish(accountId: string, value: string): Promise<void> {
    while (this.gate) await this.gate;
    const send = this.producer.send({ topic: this.topic, messages: [{ key: accountId, value }] });
    this.inFlight.add(send);
    try {
      await send;
    } finally {
      this.inFlight.delete(send);
    }
  }

  async expandPartitions(count: number): Promise<void> {
    let open!: () => void;
    this.gate = new Promise((resolve) => (open = resolve));
    const admin = this.kafka.admin();
    try {
      // 1. Yeni yazma yok; yoldaki yazmalar bitsin.
      await Promise.allSettled([...this.inFlight]);
      await admin.connect();

      // 2. Drain: eski mapping ile yazılmış her şey işlensin. Yoksa taşınan bir hesabın yeni olayı (yeni partition)
      //    eski partition'da bekleyen önceki olaylarından önce işlenebilir.
      await waitFor(async () => (await this.lag(admin)) === 0, {
        timeoutMs: 60_000,
        intervalMs: 200,
        message: `${this.consumerGroupId} drain olmadı`,
      });

      // 3. Partition'ları ekle ve leader'ları belli olana kadar bekle.
      await admin.createPartitions({ topicPartitions: [{ topic: this.topic, count }] });
      await waitFor(async () => {
        const meta = await admin.fetchTopicMetadata({ topics: [this.topic] });
        const parts = meta.topics[0].partitions;
        return parts.length === count && parts.every((p) => p.leader >= 0);
      }, { timeoutMs: 15_000, message: 'yeni partition’lar hazır olmadı' });

      // 4. Producer partition sayısını metadata cache'inden okur (metadataMaxAge, varsayılan 5 dk).
      //    Yeni producer yeni sayıyı hemen görür; eskisi bir süre eski mapping ile yazmaya devam ederdi.
      const fresh = this.kafka.producer({ idempotent: true });
      await fresh.connect();
      const old = this.producer;
      this.producer = fresh;
      await old.disconnect();
    } finally {
      await admin.disconnect().catch(() => undefined);
      this.gate = null;
      open();
    }
  }

  /** Grubun henüz işleyip commit etmediği mesaj sayısı (bkz. 14). */
  private async lag(admin: Admin): Promise<number> {
    const [committed] = await admin.fetchOffsets({ groupId: this.consumerGroupId, topics: [this.topic] });
    const latest = await admin.fetchTopicOffsets(this.topic);
    return latest.reduce((sum, p) => {
      const c = committed.partitions.find((x) => x.partition === p.partition)?.offset ?? '-1';
      const from = c === '-1' ? BigInt(p.low) : BigInt(c);
      return sum + Number(BigInt(p.high) - from);
    }, 0);
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }
}
