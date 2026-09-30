import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Module, type INestApplicationContext, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ClientsModule } from '@nestjs/microservices';
import { createKafka } from '../../src/lib/kafka.js';
import { createTopic, uniqueName } from '../../src/lib/topics.js';
import { loadImpl } from '../../src/lib/impl.js';
import { readAll } from '../../src/lib/messages.js';
import { waitFor } from '../../src/lib/wait.js';
import { KAFKA_CLIENT, LEDGER, MESSAGE_ID_HEADER, PAYMENTS_TOPIC, type Ledger, type PaymentCaptured } from './types.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);

/** Test kontrollü ledger: messageId ile idempotent, istenen ödemede bir kez hata verir. */
class FakeLedger implements Ledger {
  readonly calls: Array<{ messageId: string; paymentId: string }> = [];
  readonly appliedOrder: string[] = [];
  private readonly applied = new Set<string>();
  readonly failOnce = new Set<string>();

  async apply(messageId: string, event: PaymentCaptured) {
    this.calls.push({ messageId, paymentId: event.paymentId });
    if (this.failOnce.delete(event.paymentId)) throw new Error('ledger db timeout');
    if (this.applied.has(messageId)) return 'duplicate' as const;
    this.applied.add(messageId);
    this.appliedOrder.push(event.paymentId);
    return 'applied' as const;
  }
  callsFor = (paymentId: string) => this.calls.filter((c) => c.paymentId === paymentId).length;
}

describe('15 - NestJS Kafka entegrasyonu', () => {
  const kafka = createKafka();
  const admin = kafka.admin();
  const ledger = new FakeLedger();
  const groupId = uniqueName('payments-service');
  let groupsBefore: string[] = [];
  let microservice: INestMicroservice;
  let publisherApp: INestApplicationContext;
  let publisher: InstanceType<typeof impl.PaymentsPublisher>;

  const event = (accountId: string, paymentId = `pay-${randomUUID().slice(0, 6)}`): PaymentCaptured => ({
    paymentId,
    accountId,
    amount: 1000,
  });

  beforeAll(async () => {
    await createTopic(kafka, { name: PAYMENTS_TOPIC, partitions: 6 });
    await admin.connect();
    groupsBefore = (await admin.listGroups()).groups.map((g) => g.groupId);

    class ConsumerModule {}
    Module({ controllers: [impl.PaymentsController], providers: [{ provide: LEDGER, useValue: ledger }] })(ConsumerModule);
    microservice = await NestFactory.createMicroservice(ConsumerModule, { ...impl.microserviceOptions(groupId), logger: false });
    await microservice.listen();

    class PublisherModule {}
    Module({
      imports: [ClientsModule.register([{ name: KAFKA_CLIENT, ...impl.clientOptions() } as never])],
      providers: [impl.PaymentsPublisher],
    })(PublisherModule);
    publisherApp = await NestFactory.createApplicationContext(PublisherModule, { logger: false });
    publisher = publisherApp.get(impl.PaymentsPublisher);
  });

  afterAll(async () => {
    await publisherApp?.close();
    await microservice?.close();
    await admin.disconnect();
  });

  it('publisher: olaylar hesap key’i ve message-id header’ı ile yazılır', async () => {
    const published: Array<{ id: string; e: PaymentCaptured }> = [];
    for (const acc of ['acc-1', 'acc-2', 'acc-1', 'acc-2', 'acc-1', 'acc-2']) {
      const e = event(acc);
      const id = randomUUID();
      published.push({ id, e });
      await publisher.publishCaptured(id, e);
    }
    const messages = await readAll(kafka, PAYMENTS_TOPIC, { expected: 6 });

    for (const { id, e } of published) {
      const m = messages.find((x) => JSON.parse(x.value!).paymentId === e.paymentId);
      expect(m, `${e.paymentId} topic’te yok`).toBeDefined();
      expect(m!.key, 'key hesap id olmalı').toBe(e.accountId);
      expect(m!.headers[MESSAGE_ID_HEADER]).toBe(id);
    }
    for (const acc of ['acc-1', 'acc-2']) {
      const partitions = new Set(messages.filter((m) => m.key === acc).map((m) => m.partition));
      expect(partitions.size, `${acc} olayları tek partition’da olmalı`).toBe(1);
    }
  });

  it('Kafka’daki consumer group adı verilen groupId; publisher gereksiz consumer group açmaz', async () => {
    await waitFor(async () => {
      const { groups } = await admin.describeGroups([groupId]);
      return groups[0]?.state === 'Stable';
    }, { timeoutMs: 20_000, message: `"${groupId}" adlı aktif bir grup yok` }).catch(() => undefined);

    const groups = (await admin.listGroups()).groups.map((g) => g.groupId);
    const created = groups.filter((g) => !groupsBefore.includes(g) && !g.startsWith('lab.reader.'));
    expect(created, 'bu testin açtığı consumer group’lar').toEqual([groupId]);
  });

  it('aynı message-id iki kez gelirse deftere bir kez işlenir', async () => {
    const e = event('acc-3');
    const id = randomUUID();
    await publisher.publishCaptured(id, e);
    await publisher.publishCaptured(id, e);

    await waitFor(() => ledger.callsFor(e.paymentId) >= 2, { timeoutMs: 20_000, message: 'handler iki mesajı da almadı' });
    expect(ledger.appliedOrder.filter((p) => p === e.paymentId)).toHaveLength(1);
  });

  it('geçici hata: mesaj kaybolmaz, tekrar teslim edilir, sıra korunur', async () => {
    const flaky = event('acc-4');
    const after = event('acc-4');
    ledger.failOnce.add(flaky.paymentId);
    await publisher.publishCaptured(randomUUID(), flaky);
    await publisher.publishCaptured(randomUUID(), after);

    await waitFor(() => ledger.appliedOrder.includes(after.paymentId), { timeoutMs: 30_000 }).catch(() => undefined);
    const acc4 = ledger.appliedOrder.filter((p) => p === flaky.paymentId || p === after.paymentId);
    expect(acc4, 'başarısız olan mesaj kayboldu ya da sıra bozuldu').toEqual([flaky.paymentId, after.paymentId]);
    expect(ledger.callsFor(flaky.paymentId)).toBe(2);
  });

  it('işlenen tüm mesajların offset’i grup adına commit edilmiş', async () => {
    await waitFor(async () => {
      const [committed] = await admin.fetchOffsets({ groupId, topics: [PAYMENTS_TOPIC] });
      const latest = await admin.fetchTopicOffsets(PAYMENTS_TOPIC);
      return latest.every((p) => {
        const c = committed.partitions.find((x) => x.partition === p.partition)!.offset;
        return p.high === '0' || c === p.high;
      });
    }, { timeoutMs: 15_000, message: 'commit edilmemiş offset var' });
  });
});
