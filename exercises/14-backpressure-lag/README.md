# 14 - Backpressure ve lag

## Konu
Downstream (webhook hedefi, 3. parti API, DB) geçici olarak çöktüğünde consumer ne yapmalı?

| Tepki | Sonuç |
|-------|-------|
| Hatayı yut, devam et | **Veri kaybı** |
| Hemen tekrar dene (sıcak döngü) | Downstream'i daha da boğarsın, log'lar dolar |
| DLQ/retry topic'e at (10) | Sıra bozulur. Kesinti tüm mesajları etkiliyorsa hepsi retry'a gider |
| **Partition'ı durdur, bekle, devam et** | Kayıp yok, sıra korunur. Birikme Kafka'da olur ve lag olarak görünür |

Kafka'nın gücü burada: mesajlar zaten kalıcı. Consumer durduğunda biriken iş **lag** olarak ölçülür ve alarm kurulur.

**kafkajs deseni:**
```ts
eachMessage: async ({ message, pause }) => {
  try { await sink(message) }
  catch (e) {
    const resume = pause();             // sadece bu partition
    setTimeout(resume, e.retryAfterMs);
    throw e;                            // offset ilerlemesin, mesaj resume sonrası tekrar gelsin
  }
}
```

**Lag** = partition başına `high watermark - committed offset`. Commit yoksa grup `low watermark`'tan başlar.

## Görev (`exercise.ts`)
1. `startResilientConsumer`: Kesinti sırasında mesaj kaybetme, sırayı bozma, `retryAfterMs`'e uy.
2. `getConsumerLag`: Grubun topic üzerindeki toplam lag'i.

## İpuçları
- `admin.fetchOffsets({ groupId, topics: [topic] })`: commit edilmiş offset'ler (`'-1'` = commit yok)
- `admin.fetchTopicOffsets(topic)`: partition başına `low` / `high`

## Düşün
- `pause()` sadece o partition'ı durdurur. Downstream tamamen çöktüyse tüm partition'ları durdurmak için ne yaparsın? (`consumer.pause([{ topic }])`, circuit breaker)
- Uzun süreli pause sırasında consumer gruptan düşer mi? (Heartbeat'ler devam eder; kafkajs'te bunu nasıl doğrularsın?)
- Lag'e nasıl alarm kurarsın? Mutlak sayı mı, zaman bazlı lag mı (en eski işlenmemiş mesajın yaşı)?
