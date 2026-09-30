# 16 - Log compaction ve tombstone

## Konu
Şimdiye kadarki topic'ler bir **olay akışıydı**: her kayıt ayrı bir şey oldu ve `cleanup.policy=delete` ile belli bir süre/boyuttan sonra siliniyordu. Bazı topic'ler ise bir **durum tablosu** gibi: "her kullanıcının güncel profili", "her ürünün son fiyatı". Burada eski versiyonlar gereksiz, ama son versiyon sonsuza kadar kalmalı.

`cleanup.policy=compact` bunu yapar: broker'daki **log cleaner** arka planda log'u tarar ve her key için sadece en son kaydı tutar.

```
compaction öncesi:  alice:v1  bob:v1  alice:v2  carol:v1  bob:v2  carol:null
compaction sonrası:                   alice:v2                   bob:v2  carol:null
```

Bilmen gerekenler:
- **Aktif segment asla compact edilmez.** Log dosyalara (segment) bölünür; sadece kapanmış segment'ler temizlenir. Segment `segment.ms` (varsayılan 7 gün) ya da `segment.bytes` (varsayılan 1 GB) dolunca kapanır. Varsayılan ayarlarla küçük bir topic'te günlerce hiçbir şey olmaz.
- **`min.cleanable.dirty.ratio`** (varsayılan 0.5): Log'un en az bu oranı "kirli" (henüz temizlenmemiş) olmadan cleaner o partition'a dokunmaz.
- **Offset'ler değişmez**: Compaction kayıtları siler ama kalanların offset'leri aynı kalır. Log'da **boşluklar** oluşur: 0, 1, 2 yerine 17, 42, 93.
- **Tombstone**: `value: null` olan kayıt "bu key silindi" demektir. Compaction key'in önceki tüm kayıtlarını siler. Tombstone'un kendisi `delete.retention.ms` (varsayılan 1 gün) daha kalır, sonra o da gider. Bu süre, geride kalmış consumer'ların silmeyi görebilmesi için tanınan pencere.
- Compacted topic key'siz mesaj kabul etmez.

## Senaryo
Test 4 partition'lı bir profil topic'i açar (config'leri sen veriyorsun). 3 kullanıcıyı 30'ar kez günceller ve carol'ı siler. 2 sn bekler, sonra alice ile bob'u bir kez daha günceller ve carol için tekrar bir silme gönderir. Ardından broker'ın log cleaner'ının **gerçekten** çalışmasını 60 sn'ye kadar bekler (cleaner boşta kalınca 15 sn uyur). En sonda topic'in ham içeriğini okur.

## Görev (`exercise.ts`)
1. `compactedTopicConfigs`: Topic'in compact edilmesini ve bu lab'de birkaç saniye içinde temizlenebilmesini sağla. Segment'ler en geç ~1 sn'de kapanmalı.
2. `deleteProfile`: Silinen kullanıcının verisi compaction sonrasında topic'te kalmamalı.
3. `loadSnapshot`: Topic'i baştan oku, her kullanıcının son durumunu döndür. **Çağrı anındaki** her şeyi okuyunca bitsin: boş topic'te beklemesin, compaction sonrası da takılmasın.

## İpuçları
- Topic config adları: `cleanup.policy`, `segment.ms`, `min.cleanable.dirty.ratio`.
- `admin.fetchTopicOffsets(topic)` her partition için `low` ve `high` döner. `high` bir sonraki yazılacak offset'tir.
- Offset'ler string. Karşılaştırırken `BigInt` kullan: `"9" > "10"` string olarak `true`.
- Mesaj saymak ile offset'e bakmak compaction sonrası aynı sonucu vermez.

## Düşün
- `segment.ms=500` production'da neden kötü? (Her partition için saniyede iki dosya, binlerce açık file handle.)
- Tombstone `delete.retention.ms`'ten sonra kaybolursa, o süreden daha uzun kapalı kalmış bir consumer silmeyi nasıl kaçırır? Sonuç ne olur?
- `loadSnapshot` bittiği anda topic'e yeni bir güncelleme gelirse ne olur? Servis açılışında "snapshot + sonra canlı akışa devam" nasıl kurulur?
- Transactional producer (12) ile yazılmış bir topic'te son offset bir **transaction marker** olabilir; hiçbir mesajın offset'i `high - 1` olmaz. Bitiş koşulun bununla başa çıkar mı?
- Kafka'nın kendi `__consumer_offsets` topic'i de compacted. Neden?
