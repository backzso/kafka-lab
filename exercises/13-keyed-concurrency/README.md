# 13 - Partition içinde key bazlı paralellik

## Konu
Paralellik birimi partition'dır: `partitionsConsumedConcurrently` farklı partition'ları paralel işler. Ama partition sayısı azsa ya da handler yavaşsa (DB, HTTP), tek partition içinde de paralellik istersin.

Sıralama garantisi aslında **key başına** gerekir. Aynı partition'daki farklı key'ler birbirinden bağımsızdır.

```
partition 0: [a1 b1 a2 c1 b2 a3]
             a: a1 → a2 → a3   (seri)
             b: b1 → b2        (seri)   ← a, b, c paralel
             c: c1             (seri)
```

**Offset tuzağı:** Offset tek bir sayı ve "buraya kadar her şey bitti" anlamına geliyor. Paralel işlemede `b2` bitmişken `a1` hâlâ işleniyor olabilir. Bu durumda `b2`'nin offset'ini commit edersen ve crash olursa `a1` kaybolur. Sadece **kesintisiz tamamlanmış öneki** commit edebilirsin.

## Görev (`exercise.ts`)
`startKeyedConsumer`:
1. Aynı key'in mesajlarını sırayla işle.
2. Farklı key'leri paralel işle, ama aynı anda en fazla `maxConcurrency` handler çalışsın.
3. Bir handler hata verirse:
   - mesaj kaybolmasın: offset'i commit edilmesin, tekrar denensin,
   - o key'in sonraki mesajları başarısız mesajdan önce işlenmesin.

## İpuçları
- `consumer.run({ eachBatchAutoResolve: false, eachBatch: async ({ batch, resolveOffset, heartbeat }) => ... })`
- `resolveOffset(offset)`: bu offset'e kadar işlendi olarak işaretle (commit sonra yapılır).
- `eachBatch` hata fırlatırsa kafkajs son resolve edilen offset'ten itibaren tekrar dener.
- Semaphore: aktif iş sayacı ve bekleyenler kuyruğu.

## Düşün
- Önekteki bir mesaj çok yavaşsa commit ilerlemez. Crash'te ne kadar iş tekrarlanır?
- Hot key (bir key'in mesajların %90'ı olması) paralelliği nasıl etkiler?
- Confluent'in Java için "Parallel Consumer" kütüphanesi bu deseni offset bitmap'lerle genelleştirir.
