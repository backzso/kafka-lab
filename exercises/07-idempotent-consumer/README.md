# 07 - Idempotent consumer

## Konu
Kafka'da gerçekçi hedef **at-least-once teslimat + idempotent işleme**. Duplicate'ler şuralardan gelir:
- Producer retry'ları (idempotence kapalıysa ya da producer yeniden başladıysa, bkz. 04)
- Commit'ten önce crash (06)
- Rebalance: iki consumer kısa bir süre aynı partition'ı işler (09)

Klasik çözüm **dedup tablosu**: `processed_messages(consumer, message_id)` üzerinde unique constraint. Kritik kural: dedup kaydı ile iş etkisi **aynı transaction'da** olmalı.

```
BEGIN
  INSERT INTO processed_messages ... ON CONFLICT DO NOTHING RETURNING 1   -- satır yoksa: duplicate, çık
  UPDATE accounts SET balance = balance + ...
COMMIT
```

## Görev (`exercise.ts`)
`handleDeposit` fonksiyonunu düzelt. Şu anki kod **check-then-act** yapıyor, transaction kullanmıyor. Testler şunları deniyor:
1. Aynı mesajın art arda gelmesi
2. Aynı mesajın **eşzamanlı** 10 kez gelmesi
3. Bakiye güncellendikten sonra crash olup mesajın yeniden gelmesi
4. Farklı consumer adlarının birbirini etkilememesi
5. Uçtan uca: aynı olay topic'te 3 kez var

Tablolar `schema.ts` içinde. Yardımcı: `withTransaction(pool, client => ...)` (`src/lib/db.ts`).

## Bilgi testi: Redis ile dedup
`redis-dedup.ts` sık görülen bir yaklaşım: `SET key NX` → iş. Test, crash sonrası mesajın **sessizce kaybolduğunu** gösteriyor. Redis "işlendi" dedi, ama iş hiç yapılmadı. İki ayrı sistem arasında atomiklik yok.

## Düşün
- `message_id` nereden gelmeli? Kafka offset'i neden iyi bir id değildir? (Aynı olay iki kez produce edilirse iki farklı offset alır.)
- Dedup tablosu sonsuza kadar büyür. Ne zaman temizlenebilir? (Topic retention süresi)
- İş etkisi başka bir servise HTTP çağrısıysa (DB değil) ne yaparsın? (İdempotency-Key header, bkz. 08)
