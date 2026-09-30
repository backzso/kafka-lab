# 01 - Partition, key ve sıralama

## Konu
Kafka sıralamayı **sadece partition içinde** garanti eder. Aynı partition'a düşmesini istediğin mesajlara aynı **key**'i vermen gerekir; varsayılan partitioner `murmur2(key) % partitionCount` ile partition seçer. Key vermezsen mesajlar partition'lara dağıtılır ve aralarındaki sıra kaybolur.

Consumer tarafında `partitionsConsumedConcurrently` farklı partition'ları paralel işler; aynı partition içindeki mesajlar yine sırayla gelir. Yani doğru key + paralel partition tüketimi = hem sıralama hem throughput.

## Görev (`exercise.ts`)
1. `publishCustomerEvents`: Bir e-ticaret sisteminde **aynı müşterinin** tüm olayları (farklı siparişler dahil) sırayla işlenmeli. Doğru key'i seç.
2. `startCustomerConsumer`: 6 partition'lı topic'i, partition'ları paralel işleyerek tüket. Handler yavaş (50ms); tüm mesajlar sıralı ve hızlı işlenmeli.

## Test neyi kontrol ediyor?
- Aynı müşterinin tüm olayları tek bir partition'da mı?
- Her müşteri için `seq` artan sırada mı işlendi?
- 60 mesaj 2 saniyenin altında işlendi mi? (sıralı tüketim ~3sn sürer)

## İpuçları
- `orderId` ile key'lemek sipariş içi sırayı korur ama müşteri seviyesini korumaz.
- `consumer.run({ partitionsConsumedConcurrently: N, eachMessage })`

## Düşün
- Partition sayısını sonradan artırırsan ne olur? (`murmur2(key) % 6` ile `% 12` farklı partition verir: aynı key'in eski ve yeni mesajları farklı partition'larda olur ve geçiş anında sıra bozulabilir.)
- Tek bir müşteri çok fazla olay üretirse (hot key) ne olur?
