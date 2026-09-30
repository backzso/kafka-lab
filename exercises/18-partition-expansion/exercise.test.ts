import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Consumer } from 'kafkajs';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { sleep, waitFor } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

const accounts = Array.from({ length: 8 }, (_, i) => `acct-${i}`);
const PER_ACCOUNT = 80;
const EXPAND_AT = 40;

describe('18 - çalışan sistemde partition artırma', () => {
  const kafka = createKafka();
  const consumers: Consumer[] = [];
  let publisher: InstanceType<typeof impl.AccountPublisher> | undefined;
  let partitionCount = 0;
  let writtenToNew = 0;
  const consumed: Array<{ account: string; seq: number; partition: number }> = [];
  const rejected: string[] = [];

  beforeAll(async () => {
    const topic = await createTopic(kafka, { label: 'account-events', partitions: 3 });
    const groupId = uniqueName('ledger');

    // Aynı servisin iki instance'ı (pod). Yavaşlar: genişletme anında eski partition'larda birikmiş mesaj (lag) olsun.
    for (let i = 0; i < 2; i++) {
      consumers.push(
        await impl.startAccountConsumer(kafka, {
          topic,
          groupId,
          handle: async (account, value, partition) => {
            await sleep(50);
            consumed.push({ account, seq: Number(value), partition });
          },
        }),
      );
    }
    const admin = kafka.admin();
    await admin.connect();
    await waitFor(async () => {
      const group = (await admin.describeGroups([groupId])).groups[0];
      return group?.state === 'Stable' && group.members.length === 2;
    }, { timeoutMs: 30_000, message: 'consumer’lar gruba katılmadı' });

    publisher = new impl.AccountPublisher(kafka, topic, groupId);
    await publisher.connect();

    // Her hesap kendi olaylarını sırayla yazar; hesaplar birbirinden bağımsız ve eşzamanlı.
    let expansion: Promise<void> | undefined;
    await Promise.all(
      accounts.map(async (account) => {
        for (let seq = 0; seq < PER_ACCOUNT; seq++) {
          if (account === accounts[0] && seq === EXPAND_AT) expansion = publisher!.expandPartitions(6);
          await publisher!.publish(account, String(seq)).catch((e: Error) => void rejected.push(`${account}#${seq}: ${e.message}`));
        }
      }),
    );
    await expansion;

    // Yeniden teslim edilen kopyalar sayılmasın: her (hesap, seq) bir kez.
    const unique = () => new Set(consumed.map((c) => `${c.account}#${c.seq}`)).size;
    await waitFor(() => unique() >= accounts.length * PER_ACCOUNT, { timeoutMs: 60_000 }).catch(() => undefined);
    await sleep(1000);
    const offsets = await admin.fetchTopicOffsets(topic);
    partitionCount = offsets.length;
    writtenToNew = offsets.filter((p) => p.partition >= 3).reduce((sum, p) => sum + Number(p.high), 0);
    await admin.disconnect();
  });

  afterAll(async () => {
    await Promise.all(consumers.map((c) => c.disconnect()));
    await publisher?.disconnect();
  });

  it('hiçbir publish reddedilmedi ve tüm mesajlar işlendi', () => {
    expect(rejected, 'reddedilen publish çağrıları').toEqual([]);
    const missing = accounts.flatMap((a) => {
      const seen = new Set(consumed.filter((c) => c.account === a).map((c) => c.seq));
      return Array.from({ length: PER_ACCOUNT }, (_, seq) => seq).filter((s) => !seen.has(s)).map((s) => `${a}#${s}`);
    });
    const consumedFromNew = consumed.filter((c) => c.partition >= 3).length;
    const hint = `yeni partition’lara ${writtenToNew} mesaj yazıldı, oradan ${consumedFromNew} mesaj işlendi`;
    expect(missing.length, `işlenmeyen ${missing.length} mesaj; ${hint}`).toBe(0);
  });

  it('genişletmeden sonra yazılan mesajlar yeni partition’lara da gitti', () => {
    expect(partitionCount, 'topic partition sayısı').toBe(6);
    expect(writtenToNew, 'yeni partition’lara (3-5) hiç mesaj yazılmadı: producer hâlâ 3 partition olduğunu düşünüyor').toBeGreaterThan(0);
  });

  it('her hesabın olayları sırayla işlendi', () => {
    for (const account of accounts) {
      // Rebalance sonrası yeniden teslim (at-least-once) sıra bozulması sayılmaz: her olayın ilk işlenişine bakılır.
      const seen = new Set<number>();
      const events = consumed.filter((c) => c.account === account && !seen.has(c.seq) && seen.add(c.seq));
      const seqs = events.map((c) => c.seq);
      const firstBreak = seqs.findIndex((s, i) => i > 0 && s < seqs[i - 1]);
      const context =
        firstBreak === -1
          ? ''
          : `: seq ${events[firstBreak].seq} (p${events[firstBreak].partition}), seq ${events[firstBreak - 1].seq} (p${events[firstBreak - 1].partition})’dan sonra işlendi`;
      expect(firstBreak, `${account} sırası bozuk${context}`).toBe(-1);
    }
  });
});
