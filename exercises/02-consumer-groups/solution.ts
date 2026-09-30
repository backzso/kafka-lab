import type { Consumer, Kafka, KafkaMessage } from 'kafkajs';

export interface WorkerOptions {
  topic: string;
  serviceName: string;
  instanceId: string;
  onMessage: (message: KafkaMessage, partition: number) => Promise<void>;
}

export async function startWorker(kafka: Kafka, opts: WorkerOptions): Promise<Consumer> {
  // Grup = mantıksal abone (servis). Instance kimliği clientId/log için kullanılır, groupId için değil.
  const consumer = kafka.consumer({ groupId: opts.serviceName });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message, partition }) => opts.onMessage(message, partition) });
  return consumer;
}

export async function replayAll(
  kafka: Kafka,
  topic: string,
  groupId: string,
  onMessage: (message: KafkaMessage) => Promise<void>,
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  // Offset'i olmayan yeni grup için başlangıç noktası (auto.offset.reset=earliest).
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => onMessage(message) });
  return consumer;
}

export async function rewindGroupToTimestamp(
  kafka: Kafka,
  groupId: string,
  topic: string,
  timestamp: number,
): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const { groups } = await admin.describeGroups([groupId]);
    const group = groups[0];
    if (group && group.members.length > 0) {
      throw new Error(`group ${groupId} is active (${group.state}); stop consumers before rewinding`);
    }
    const offsets = await admin.fetchTopicOffsetsByTimestamp(topic, timestamp);
    await admin.setOffsets({
      groupId,
      topic,
      partitions: offsets.map(({ partition, offset }) => ({ partition, offset })),
    });
  } finally {
    await admin.disconnect();
  }
}
