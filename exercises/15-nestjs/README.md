# 15 - NestJS Kafka entegrasyonu

## Konu
`@nestjs/microservices` Kafka transport'u kafkajs'in üstünde ince bir katman. Kolaylık sağlar, ama bazı varsayılanları sessizce değiştirir. Bu lab'de doğrulanan tuzaklar:

| Tuzak | Sonuç |
|-------|-------|
| `groupId: 'payments'` yazarsın, Nest Kafka'da **`payments-server`** grubunu kullanır (`postfixId` varsayılanı `-server`, clientId'ye de eklenir) | Monitoring, offset reset, ACL'ler yanlış grubu hedefler |
| `ClientKafka`, sadece `emit` kullansan da cevaplar için bir **consumer group açar** (`<groupId>-client`) | Gereksiz grup üyeliği ve rebalance, broker üzerinde ek yük |
| `client.emit(topic, obj)` düz nesneyle çağrılırsa **key null** olur | Olaylar partition'lara dağılır, sıra garantisi kaybolur |
| `ClientKafka` producer'ı varsayılan olarak **idempotent değil** (kafkajs varsayılanı) | Retry'larda duplicate (04) |
| Handler'da `try/catch` ile hatayı yutmak | Offset commit edilir, mesaj **kaybolur** |

**Hata semantiği (lab'de doğrulandı):** Nest event handler'ı `await` eder. Hata fırlatılırsa kafkajs'e ulaşır; `eachMessage` offset'i resolve etmez ve mesaj tekrar gelir. Yani varsayılan `autoCommit` ile de at-least-once alırsın, **yeter ki hatayı yutmayasın**. Global bir exception filter hatayı yutup normal değer dönerse de aynı kayıp olur.

Manuel commit istersen: `run: { autoCommit: false }` ve handler içinde `ctx.getConsumer().commitOffsets([{ topic: ctx.getTopic(), partition: ctx.getPartition(), offset: next }])`.

## Görev (`exercise.ts`)
1. `microserviceOptions(groupId)`: Kafka'daki grup adı tam olarak `groupId` olsun.
2. `clientOptions()`: Sadece yayın yapan, retry'da duplicate üretmeyen client.
3. `PaymentsController.onPaymentCaptured`:
   - Mesaj kimliğini `message-id` header'ından al ve ledger'a ver (ledger bununla dedup yapıyor).
   - Hata olursa mesaj kaybolmasın.
4. `PaymentsPublisher.publishCaptured`: Aynı hesabın olayları aynı partition'a düşsün ve `message-id` header'ı taşısın.

## İpuçları
- `KafkaOptions['options']`: `postfixId`, `producer`, `producerOnlyMode`, `run`, `subscribe`
- `ctx.getMessage().headers`, `ctx.getTopic()`, `ctx.getPartition()`, `ctx.getConsumer()`
- `client.emit(topic, { key, value, headers })`: Nest bu alanları Kafka mesajına eşler.

## Not: decorator'lar ve DI
Testler `tsx`/`vitest` (esbuild) ile çalışıyor. esbuild `emitDecoratorMetadata` üretmez; bu yüzden constructor enjeksiyonu için `@Inject(TOKEN)` kullanılıyor. Nest CLI (`tsc`/`swc`) ile derlenen gerçek projelerde tip bazlı enjeksiyon çalışır.

## Düşün
- 07'deki "dedup kaydı ve iş etkisi aynı transaction'da" kuralını Nest'te nasıl uygularsın? Interceptor, handler'ın DB transaction'ına erişemez. (AsyncLocalStorage tabanlı transaction context, ör. `@nestjs-cls/transactional`, ya da dedup'ı servis katmanında yapmak.)
- Nest'te poison mesaj için retry topic/DLQ (10) nerede yaşamalı: exception filter mı, interceptor mı, servis mi?
