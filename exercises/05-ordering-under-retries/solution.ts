import type { Producer } from 'kafkajs';

export class OrderedPublisher {
  /** key -> o key'in son gönderiminin (hatası yutulmuş) promise'i */
  private readonly tails = new Map<string, Promise<void>>();

  constructor(
    private readonly producer: Producer,
    private readonly topic: string,
  ) {}

  publish(key: string, value: string): Promise<void> {
    const previous = this.tails.get(key) ?? Promise.resolve();
    const send = previous.then(() => this.producer.send({ topic: this.topic, messages: [{ key, value }] })).then(() => undefined);
    // Zincir hata yüzünden kilitlenmesin: sonraki halka önceki başarısız olsa da çalışır.
    const tail = send.catch(() => undefined);
    this.tails.set(key, tail);
    void tail.then(() => {
      if (this.tails.get(key) === tail) this.tails.delete(key);
    });
    return send;
  }

  pendingKeys(): number {
    return this.tails.size;
  }
}
