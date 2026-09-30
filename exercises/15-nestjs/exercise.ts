import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { Controller, Inject, Injectable } from '@nestjs/common';
import { Ctx, EventPattern, Payload, Transport, type ClientKafka, type KafkaContext, type KafkaOptions } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import { config } from '../../src/lib/config.js';
import { KAFKA_CLIENT, LEDGER, PAYMENTS_TOPIC, type Ledger, type PaymentCaptured } from './types.js';

/**
 * Consumer tarafı: NestFactory.createMicroservice(AppModule, microserviceOptions(groupId))
 * Kafka'daki consumer group adı tam olarak `groupId` olmalı. Offset'ler sadece başarıyla işlenen mesajlar için commit edilmeli.
 */
export function microserviceOptions(groupId: string): KafkaOptions {
  // TODO: Nest bu ayarlarla Kafka'da hangi group adını kullanıyor? Commit ne zaman yapılıyor?
  return {
    transport: Transport.KAFKA,
    options: {
      client: { clientId: 'payments', brokers: config.brokers, logLevel: config.kafkaLogLevel },
      consumer: { groupId },
      subscribe: { fromBeginning: true },
    },
  };
}

/**
 * Producer tarafı: ClientsModule.register([{ name: KAFKA_CLIENT, ...clientOptions() }])
 * Sadece event yayınlıyoruz (request/response yok). Retry'lar duplicate üretmemeli.
 */
export function clientOptions(): KafkaOptions {
  // TODO
  return {
    transport: Transport.KAFKA,
    options: {
      client: { clientId: 'payments-api', brokers: config.brokers, logLevel: config.kafkaLogLevel },
    },
  };
}

@Controller()
export class PaymentsController {
  constructor(@Inject(LEDGER) private readonly ledger: Ledger) {}

  @EventPattern(PAYMENTS_TOPIC)
  async onPaymentCaptured(@Payload() event: PaymentCaptured, @Ctx() ctx: KafkaContext) {
    // TODO: Mesaj kimliği nereden gelmeli? Hata olursa ne olur? Offset ne zaman commit ediliyor?
    try {
      await this.ledger.apply(randomUUID(), event);
    } catch (e) {
      console.error('ledger hatası', e);
    }
  }
}

@Injectable()
export class PaymentsPublisher {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  /** Olayı yayınlar. Aynı hesabın olayları sıralı işlenebilmeli; her olay kalıcı bir message-id taşımalı. */
  async publishCaptured(messageId: string, event: PaymentCaptured): Promise<void> {
    // TODO
    await lastValueFrom(this.client.emit(PAYMENTS_TOPIC, event));
  }
}
