# 03 - `acks` ve dayanıklılık

## Konu
Producer'ın "başarılı" cevabı ne anlama geliyor?

| acks | Leader ne zaman cevap verir? | Risk |
|------|------------------------------|------|
| `0`  | Hiç beklemez | Hata bile görmezsin |
| `1`  | Kendi log'una yazınca | Leader, follower'lar kopyalamadan ölürse onaylanmış mesaj **kaybolur** |
| `-1` (`all`) | ISR'daki tüm replikalar yazınca | ISR < `min.insync.replicas` ise yazma reddedilir |

Bu lab'deki cluster: 3 broker, topic'ler `replication.factor=3`, `min.insync.replicas=2`.

**ISR (in-sync replicas):** Leader'a yetişmiş replikalar. Yeni leader sadece ISR'dan seçilir (`unclean.leader.election.enable=false`). ISR'da olan ama mesajı henüz almamış bir follower leader olursa, eski leader'ın onayladığı mesajlar silinir (log truncation).

## Senaryo (test bunu gerçekten yapıyor)
1. Follower'lar `docker pause` ile ~2sn dondurulur (ISR'dan düşmeleri için 10sn gerekir, düşmezler).
2. 20 mesaj senin `durableSend` fonksiyonunla gönderilir.
3. Leader `docker kill` ile öldürülür, follower'lar çözülür.
4. Controller eski leader'ı fence eder ve ISR'daki bir follower'ı leader yapar.
5. Leader geri gelir, yeni leader'a göre log'unu keser.
6. **Başarılı dönen her gönderim topic'te olmalı.**

İkinci test: iki follower tamamen durdurulur, ISR=1 olur. `acks=all` yazmalar reddedilir. Hatayı doğru sınıflandırman gerekiyor.

## Görev (`exercise.ts`)
1. `durableSend`: Onaylanan mesaj kaybolmamalı.
2. `describeFailure`: Producer'dan gelen hatanın **asıl sebebini** bul: protokol hata tipi (`NOT_ENOUGH_REPLICAS` gibi) ve retriable olup olmadığı.

## İpuçları
- `producer.send({ topic, messages, acks })`
- kafkajs retry'lar bitince hatayı `KafkaJSNumberOfRetriesExceeded` ile sarar ve bu wrapper `retriable: false` der. Asıl hata `error.cause` zincirinde (`KafkaJSProtocolError`, alanları: `type`, `retriable`).

## Düşün
- `acks=all` + `min.insync.replicas=1` neden neredeyse `acks=1` kadar risklidir?
- `NOT_ENOUGH_REPLICAS` retriable'dır ama sonsuza kadar retry etmek doğru mu? Upstream'e (HTTP isteğine) ne dönmeli?
- `acks=1` hız kazandırır; hangi veri türleri için kabul edilebilir? (metrikler, clickstream)
