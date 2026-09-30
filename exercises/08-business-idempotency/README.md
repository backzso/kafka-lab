# 08 - İş seviyesinde idempotency

## Konu
07'deki dedup **aynı mesajın** tekrarını yakalar. Ama duplicate'lerin bir kısmı Kafka'dan değil, **kullanıcıdan** gelir:

```
Mobil uygulama ──POST /payments (Idempotency-Key: abc)──▶ API ──▶ Kafka (commandId: 1)
         (timeout, cevap gelmedi, uygulama retry eder)
Mobil uygulama ──POST /payments (Idempotency-Key: abc)──▶ API ──▶ Kafka (commandId: 2)
```

İki farklı mesaj ve iki farklı `commandId`. Mesaj bazlı dedup ikisini de işler ve müşteriden iki kez para çekilir.

Çözüm: istemcinin gönderdiği **Idempotency-Key**'i iş tablosunda unique constraint olarak kullanmak (Stripe'ın modeli):
- Aynı key + aynı içerik: orijinal sonucu dön (aynı `paymentId`). İstemci retry'ı ile ilk isteği ayırt edemez; ikisine de aynı cevabı vermelisin.
- Aynı key + farklı içerik: istemci hatası (`409 Conflict` / `422`). Sessizce yeni ödeme oluşturma, eskisini de dönme.
- Key'in kapsamı: genelde hesap/müşteri. Global unique olmak zorunda değil.

## Görev (`exercise.ts`)
1. `ensureSchema`: Gereken constraint'i ekle.
2. `processPayment`: Kurallara göre `created` / `duplicate` / `conflict` dön.

## İpuçları
- `INSERT ... ON CONFLICT (cols) DO NOTHING RETURNING ...` → satır dönmediyse mevcut kaydı oku.
- "Aynı içerik" kararı için isteğin anlamlı alanlarından bir hash (`request_hash`) sakla. `commandId` bu hash'e dahil olmamalı.

## Düşün
- Natural key (ör. `orderId` başına tek ödeme) varsa ayrıca idempotency key'e gerek var mı?
- İlk istek hâlâ işlenirken retry gelirse? (Bu testte INSERT atomik olduğu için sorun yok. İş etkisi dış bir çağrıysa "processing" durumu gerekir.)
