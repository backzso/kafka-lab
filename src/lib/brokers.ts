import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { Kafka } from 'kafkajs';
import { brokerServices, config } from './config.js';
import { waitFor } from './wait.js';

const exec = promisify(execFile);

async function compose(...args: string[]) {
  await exec('docker', ['compose', '-p', config.composeProject, ...args], { cwd: process.cwd() });
}

function service(nodeId: number): string {
  const s = brokerServices[nodeId];
  if (!s) throw new Error(`bilinmeyen broker id: ${nodeId}`);
  return s;
}

/** docker compose üzerinden broker kontrolü. Lab ortamı izole olduğu için güvenle kullanılabilir. */
export const brokers = {
  /** SIGKILL: broker temiz kapanmaz, leadership devri yapmaz. */
  kill: (...ids: number[]) => compose('kill', ...ids.map(service)),
  stop: (...ids: number[]) => compose('stop', '-t', '1', ...ids.map(service)),
  start: (...ids: number[]) => compose('start', ...ids.map(service)),
  /** Process dondurulur: TCP bağlantıları açık kalır ama cevap gelmez. */
  pause: (...ids: number[]) => compose('pause', ...ids.map(service)),
  unpause: (...ids: number[]) => compose('unpause', ...ids.map(service)),

  /** Durdurulmuş/dondurulmuş tüm broker'ları geri getirir ve cluster'ın 3 broker ile hazır olmasını bekler. */
  async restoreAll(kafka: Kafka, topics: string[] = []) {
    await compose('unpause', 'kafka-1', 'kafka-2', 'kafka-3').catch(() => undefined);
    await compose('start', 'kafka-1', 'kafka-2', 'kafka-3');
    await waitForHealthy(kafka, topics);
  },
};

/** 3 broker cluster'da görünene ve verilen topic'lerin ISR'ı tamamlanana kadar bekler. */
export async function waitForHealthy(kafka: Kafka, topics: string[] = [], timeoutMs = 90_000) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await waitFor(async () => {
      const cluster = await admin.describeCluster();
      if (cluster.brokers.length !== 3) return false;
      if (!topics.length) return true;
      const meta = await admin.fetchTopicMetadata({ topics });
      return meta.topics.every((t) => t.partitions.every((p) => p.leader >= 0 && p.isr.length === p.replicas.length));
    }, { timeoutMs, intervalMs: 500, message: 'cluster sağlıklı hale gelmedi' });
  } finally {
    await admin.disconnect();
  }
}
