import 'reflect-metadata';
import { Controller, Inject, Injectable } from '@nestjs/common';
import { Ctx, EventPattern, Payload, Transport, type ClientKafka, type KafkaContext, type KafkaOptions } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import { config } from '../../src/lib/config.js';
import { KAFKA_CLIENT, LEDGER, MESSAGE_ID_HEADER, PAYMENTS_TOPIC, type Ledger, type PaymentCaptured } from './types.js';

export function microserviceOptions(groupId: string): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      client: { clientId: 'payments', brokers: config.brokers, logLevel: config.kafkaLogLevel },
      consumer: { groupId },
      // Nest varsayılan olarak clientId ve groupId'ye "-server" ekler; Kafka'daki grup adı "<groupId>-server" olur.
      postfixId: '',
      subscribe: { fromBeginning: true },
    },
  };
}

export function clientOptions(): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      client: { clientId: 'payments-api', brokers: config.brokers, logLevel: config.kafkaLogLevel },
      producer: { idempotent: true, maxInFlightRequests: 1 },
      // Sadece emit: ClientKafka'nın cevap dinlemek için açtığı consumer group'a gerek yok.
      producerOnlyMode: true,
    },
  };
}

@Controller()
export class PaymentsController {
  constructor(@Inject(LEDGER) private readonly ledger: Ledger) {}

  @EventPattern(PAYMENTS_TOPIC)
  async onPaymentCaptured(@Payload() event: PaymentCaptured, @Ctx() ctx: KafkaContext) {
    const message = ctx.getMessage();
    const messageId = message.headers?.[MESSAGE_ID_HEADER]?.toString();
    if (!messageId) throw new Error(`${MESSAGE_ID_HEADER} header missing`);

    // Hata yakalanmadan kafkajs'e ulaşmalı: offset resolve edilmez ve mesaj tekrar teslim edilir.
    await this.ledger.apply(messageId, event);
  }
}

@Injectable()
export class PaymentsPublisher {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  async publishCaptured(messageId: string, event: PaymentCaptured): Promise<void> {
    // { key, value, headers } şeklinde verilirse Nest bunları Kafka mesajının alanları olarak kullanır;
    // düz nesne verilirse key null olur ve olaylar partition'lara dağılır.
    await lastValueFrom(
      this.client.emit(PAYMENTS_TOPIC, {
        key: event.accountId,
        value: event,
        headers: { [MESSAGE_ID_HEADER]: messageId },
      }),
    );
  }
}
