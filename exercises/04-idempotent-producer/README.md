# 04 - İdempotent producer

## Konu
Ağ, "yazıldı mı?" sorusunun cevabını kaybedebilir:

```
producer ──Produce(charge-42)──▶ broker   (log'a yazıldı)
producer ◀──── ✗ bağlantı koptu ──── broker
producer ──Produce(charge-42)──▶ broker   (retry: log'a TEKRAR yazıldı)
```

Producer retry etmezse mesaj kaybolabilir; retry ederse duplicate oluşabilir. **İdempotent producer** bunu broker tarafında çözer:
- Producer'a `producerId` (PID) atanır, her partition'a giden her batch'e artan bir `sequence` numarası eklenir.
- Broker, aynı PID + sequence'ı ikinci kez görürse yazmaz, önceki sonucu döner.
- `acks=all` gerektirir.

**Sınır:** Koruma sadece aynı producer oturumu içindeki retry'ları kapsar. Uygulama çöküp yeniden başlarsa, ya da HTTP katmanında kullanıcı "tekrar dene"ye basarsa yeni bir gönderim olur: broker bunu duplicate olarak tanımaz. Buna karşı consumer tarafında idempotency gerekir (07 ve 08).

## Senaryo
Test, kafkajs'e takılan bir chaos soketi kullanıyor. Soket Produce isteğini broker'a iletiyor, broker yazıyor, ama yanıt client'a ulaşmadan bağlantıyı koparıyor. kafkajs bağlantı hatasını retriable sayıp tekrar gönderiyor.

## Görev (`exercise.ts`)
`createPaymentProducer`: Ödeme olaylarını yazacak producer'ı oluştur. Retry'lar duplicate üretmemeli.

## Düşün
- Kafka 3.0+ Java client'ta `enable.idempotence=true` varsayılan. kafkajs'te değil.
- `maxInFlightRequests` ile idempotence arasında nasıl bir ilişki var? (bkz. 05)
- Son testteki "uygulama seviyesinde tekrar"ı nasıl engellersin?
