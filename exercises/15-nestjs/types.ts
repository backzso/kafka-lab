import { randomUUID } from 'node:crypto';

/**
 * Decorator'lar (`@EventPattern`) sabit topic adı ister. Test her çalıştırmada benzersiz bir ad
 * üretip bu modül yüklenmeden önce EX15_TOPIC'e yazar.
 */
export const PAYMENTS_TOPIC = process.env.EX15_TOPIC ?? `lab.nest.payments.${randomUUID().slice(0, 8)}`;

/** DI token'ları. (tsx/vitest esbuild kullanır, emitDecoratorMetadata yok: enjeksiyon için @Inject(token) şart.) */
export const LEDGER = Symbol('LEDGER');
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

export interface PaymentCaptured {
  paymentId: string;
  accountId: string;
  amount: number;
}

/** Kafka header'ında taşınan mesaj kimliği. */
export const MESSAGE_ID_HEADER = 'message-id';

export interface Ledger {
  /**
   * Ödemeyi deftere işler. messageId ile idempotent'tir (07'deki desen): aynı messageId ikinci kez gelirse etkisizdir.
   * Geçici hatalarda fırlatır; mesaj tekrar denenmeli.
   */
  apply(messageId: string, event: PaymentCaptured): Promise<'applied' | 'duplicate'>;
}
