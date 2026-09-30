# 18 - Çalışan sistemde partition artırma

## Konu
Throughput yetmiyor, consumer'ları artırmak istiyorsun, ama bir gruptaki aktif consumer sayısı partition sayısını geçemez. Çözüm partition eklemek gibi görünür. Kafka partition sayısını **artırmaya** izin verir (azaltmaya değil) ve komutu tek satırdır. Asıl sorunlar komuttan sonra başlar.

**1. Key'ler taşınır.** Varsayılan partitioner `murmur2(key) % partitionSayısı`. Sayı 3'ten 6'ya çıkınca yaklaşık her iki key'den biri başka partition'a gider:

```
acct-2: murmur2 % 3 = 2  →  murmur2 % 6 = 5
        eski olaylar p2'de (henüz işlenmemiş, lag var)
        yeni olaylar p5'te (boş, hemen işlenir)  →  sıra bozulur
```
Bu testteki 8 hesabın 5'i taşınıyor (acct-2, 3, 4, 6, 7).
Kafka sıralamayı sadece partition **içinde** garanti eder (01). Key'in eski ve yeni partition'ı arasında bir garanti yok.

**2. Client'lar değişikliği hemen görmez.** Producer ve consumer partition sayısını metadata cache'inden okur. kafkajs'te `metadataMaxAge` varsayılanı **5 dakika**:
- Açık producer 5 dakikaya kadar eski sayıyla (`% 3`) yazmaya devam eder, sonra aniden `% 6`'ya geçer. Bu geçiş anını kimse kontrol etmez.
- Consumer yeni partition'ları 5 dakikaya kadar okumaz. Oraya yazılan mesajlar bekler.

**3. Yeni partition'da commit edilmiş offset yok.** Consumer onu fark ettiğinde `auto.offset.reset` devreye girer. `fromBeginning: false` (latest) ise, consumer fark edene kadar oraya yazılmış her şey **sessizce atlanır**.

## Senaryo
3 partition'lı topic, aynı gruptan 2 consumer instance'ı (her mesaj 50 ms) ve 8 hesap. Her hesap kendi olaylarını sırayla yazar (`seq` 0..79). Hesap 0 `seq=40`'a gelince test `expandPartitions(6)` çağırır. Bu sırada diğer hesaplar yazmaya devam eder. Consumer'lar yavaş olduğu için genişletme anında eski partition'larda yüzlerce mesaj birikmiştir.

Test kontrol eder:
- Hiçbir `publish` reddedilmedi ve her mesaj işlendi.
- Genişletmeden sonra yeni partition'lara (3-5) gerçekten mesaj yazıldı.
- Her hesabın olayları `seq` sırasıyla işlendi (rebalance kaynaklı yeniden teslimler hariç).

## Görev (`exercise.ts`)
1. `startAccountConsumer`: Yeni partition'ları makul sürede (birkaç saniye) fark etsin ve oradaki hiçbir mesajı atlamasın.
2. `AccountPublisher.expandPartitions`: Partition sayısını artır. Bu sırada gelen `publish` çağrıları reddedilmesin, ama hiçbir hesabın sırası bozulmasın. İşlem bittiğinde publisher yeni partition'ları kullanıyor olsun.

## İpuçları
- Consumer: `metadataMaxAge` ve `subscribe({ fromBeginning })`.
- Sırayı korumanın yolu: eski mapping'le yazılmış **her şey** işlendikten sonra yeni mapping'le yazmaya başlamak. Bunun için yazmayı durdurmak ve beklemek gerekir.
- "Her şey işlendi" = consumer group lag'i 0 (14'teki hesap). Grubun id'si constructor'da.
- Durdurmadan önce yola çıkmış (`send` edilmiş ama sonucu gelmemiş) mesajlar da var.
- Producer'ın metadata'sını yenilemenin en garantili yolu yeni bir producer.
- Beklemedeki `publish` çağrıları bir Promise'i bekleyebilir. Genişletme bitince (hata olsa bile) serbest bırakılmalılar.

## Düşün
- Bu çözüm yazmayı saniyelerce durduruyor. Upstream (HTTP API) bu sürede ne yapmalı? Lag çok büyükse ne olur?
- Publisher'ın birden fazla instance'ı (pod) varsa "yazmayı durdur" nasıl koordine edilir?
- Alternatif 1: Yeni partition sayısıyla **yeni bir topic** aç, consumer'ları iki topic'i de okuyacak şekilde geçir, producer'ları taşı, eski topic'i drain et.
- Alternatif 2: Baştan fazla partition aç (ör. 12 ya da 24), ihtiyaç arttıkça consumer ekle. Partition başına maliyet ne? (Broker'da dosya, bellek, leader election süresi.)
- Consumer key başına bellekte durum tutuyorsa (ör. hesap bakiyesi cache'i), key başka partition'a, yani başka bir pod'a taşınınca ne olur?
- Log compaction (16) key bazlı çalışır. Key'ler taşındıktan sonra compacted bir topic'te aynı key'in iki partition'da kayıtları olur. Snapshot okuyan biri için sorun olur mu?
