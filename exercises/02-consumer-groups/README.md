# 02 - Consumer group temelleri

## Konu
- Aynı `groupId`'ye sahip consumer'lar partition'ları **paylaşır**: her partition gruptaki tek bir üyeye atanır, her mesaj grup içinde bir kez işlenir.
- Farklı `groupId` = bağımsız abonelik: her grup her mesajı alır (pub/sub).
- Offset'ler **grup başına** saklanır (`__consumer_offsets`). Grubun commit edilmiş offset'i varsa `fromBeginning` **dikkate alınmaz**; sadece offset'i olmayan yeni gruplar için geçerlidir (`auto.offset.reset`).
- Hatalı bir deploy sonrası "şu saatten itibaren yeniden işle" ihtiyacı için grubun offset'leri timestamp'e göre geri sarılabilir. Bu işlem sadece grup **boşken** (aktif üye yokken) yapılabilir.

## Görev (`exercise.ts`)
1. `startWorker`: Aynı servisin birden fazla instance'ı işi paylaşmalı. Şu anki kod her instance'a ayrı grup veriyor: her mesaj N kez işleniyor.
2. `replayAll`: Topic'teki **mevcut** tüm mesajları okuyan bir consumer başlat.
3. `rewindGroupToTimestamp`: Grubun offset'lerini verilen zamana geri sar. Grup aktifse anlamlı bir hata fırlat (mesajında "active" geçsin).

## İpuçları
- `admin.fetchTopicOffsetsByTimestamp(topic, ts)` → her partition için o zamandan sonraki ilk offset.
- `admin.setOffsets({ groupId, topic, partitions })`
- `admin.describeGroups([groupId])` → `state` (`Empty`, `Stable`, ...) ve `members`.

## Düşün
- Kubernetes'te pod adını groupId'ye koymak neden sık yapılan bir hatadır?
- Partition sayısından fazla consumer başlatırsan ne olur?
