import type { Producer } from 'kafkajs';

/**
 * Eşzamanlı çağrılabilen publisher. Aynı key'in mesajları çağrı sırasıyla yazılmalı,
 * farklı key'ler birbirini beklememeli.
 */
export class OrderedPublisher {
  constructor(
    private readonly producer: Producer,
    private readonly topic: string,
  ) {}

  async publish(key: string, value: string): Promise<void> {
    // TODO: Eşzamanlı çağrılarda sıra korunuyor mu?
    await this.producer.send({ topic: this.topic, messages: [{ key, value }] });
  }

  /** İçeride takip edilen (işi devam eden) key sayısı. */
  pendingKeys(): number {
    return 0;
  }
}
