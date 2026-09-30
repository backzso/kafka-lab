import { randomUUID } from 'node:crypto';
import type { Admin, Kafka } from 'kafkajs';
import { config } from './config.js';
import { waitFor } from './wait.js';

/** `lab.` ön ekli, testler arasında çakışmayan isim üretir. */
export function uniqueName(label: string): string {
  return `${config.topicPrefix}${label}.${randomUUID().slice(0, 8)}`;
}

export interface TopicSpec {
  name?: string;
  label?: string;
  partitions?: number;
  replicationFactor?: number;
  configs?: Record<string, string>;
  /** Partition başına replika listesi; ilk eleman tercih edilen leader. Verilirse partitions/replicationFactor yok sayılır. */
  replicaAssignment?: number[][];
}

/** Topic oluşturur ve tüm partition'ların leader'ı belli olana kadar bekler. */
export async function createTopic(kafka: Kafka, spec: TopicSpec = {}): Promise<string> {
  const name = spec.name ?? uniqueName(spec.label ?? 'topic');
  const partitions = spec.replicaAssignment?.length ?? spec.partitions ?? 1;
  const admin = kafka.admin();
  await admin.connect();
  try {
    const configEntries = Object.entries(spec.configs ?? {}).map(([n, value]) => ({ name: n, value }));
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        spec.replicaAssignment
          ? {
              topic: name,
              replicaAssignment: spec.replicaAssignment.map((replicas, partition) => ({ partition, replicas })),
              configEntries,
            }
          : { topic: name, numPartitions: partitions, replicationFactor: spec.replicationFactor ?? 3, configEntries },
      ],
    });
    await waitFor(async () => {
      const meta = await admin.fetchTopicMetadata({ topics: [name] });
      const parts = meta.topics[0]?.partitions ?? [];
      return parts.length === partitions && parts.every((p) => p.leader >= 0);
    }, { timeoutMs: 15_000, message: `topic ${name} hazır olmadı` });
  } finally {
    await admin.disconnect();
  }
  return name;
}

export async function createTopics(kafka: Kafka, specs: TopicSpec[]): Promise<string[]> {
  const names: string[] = [];
  for (const spec of specs) names.push(await createTopic(kafka, spec));
  return names;
}

export async function partitionLeader(admin: Admin, topic: string, partition = 0) {
  const meta = await admin.fetchTopicMetadata({ topics: [topic] });
  const p = meta.topics[0].partitions.find((x) => x.partitionId === partition);
  if (!p) throw new Error(`partition ${partition} bulunamadı`);
  return { leader: p.leader, replicas: p.replicas, isr: p.isr };
}

/** `lab.` ön ekli tüm topic'leri siler. */
export async function deleteLabTopics(kafka: Kafka): Promise<number> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const topics = (await admin.listTopics()).filter((t) => t.startsWith(config.topicPrefix));
    if (topics.length) await admin.deleteTopics({ topics, timeout: 30_000 });
    return topics.length;
  } finally {
    await admin.disconnect();
  }
}
