# Kafka Lab

Kafka'da teslim garantileri, duplicate'ler ve hata senaryoları üzerine pratik laboratuvarı. Her egzersizde TODO'lu bir başlangıç kodu (`exercise.ts`), seni test eden otomatik testler (`exercise.test.ts`) ve referans bir çözüm (`solution.ts`) var. Testler mock kullanmıyor: gerçek bir 3 broker'lı cluster üzerinde leader öldürüyor, process'leri `SIGKILL`/`SIGSTOP` ile durduruyor ve ağ yanıtlarını yutuyor.

## Kurulum

```bash
npm install
npm run infra:up      # docker compose up + cluster'ın hazır olmasını bekler
```

| Servis | Adres |
|--------|-------|
| Kafka (3 broker, KRaft) | `localhost:19092,19093,19094` |
| Kafka UI | http://localhost:18080 |
| Postgres | `postgres://lab:lab@localhost:15432/kafka_lab` |
| Redis | `redis://localhost:16379` |

Portlar global servislerle çakışmaz. Topic'ler `replication.factor=3` ve `min.insync.replicas=2` ile açılıyor. Controller ayrı bir node; broker'ları durdurmak quorum'u bozmaz.

## Çalışma akışı

```bash
npm run ex 06              # exercises/06-* testlerini SENİN kodunla (exercise.ts) çalıştırır
npm run ex:solution 06     # aynı testleri referans çözümle çalıştırır
IMPL=attempt2 npm run ex 06   # exercises/06-*/attempt2.ts dosyasını test eder (alternatif denemeler için)
npm run test:solutions     # tüm çözümler (~5 dk)
npm run clean              # lab.* topic'lerini siler
```

Her egzersizde önce `README.md` dosyasını oku, sonra `exercise.ts`'i düzelt. Testlerin hata mesajları neyin yanlış gittiğini anlatacak şekilde yazıldı.

## Müfredat

| # | Konu | Test ne yapıyor? |
|---|------|------------------|
| 01 | Partition, key, sıralama | Müşteri bazında sıra + paralel partition tüketimi |
| 02 | Consumer group | Paylaşılan grup, `fromBeginning`, offset'i timestamp'e geri sarma |
| 03 | `acks` ve dayanıklılık | Follower'ları dondurup leader'ı öldürür; `acks=1` ile onaylanan mesajlar kaybolur |
| 04 | İdempotent producer | Produce yanıtını yutup bağlantıyı koparır; retry duplicate üretir |
| 05 | Retry altında sıralama | kafkajs'te eşzamanlı `send()` sırası hiçbir konfigürasyonla korunmuyor |
| 06 | Offset commit semantiği | Consumer process'i işlem ortasında `SIGKILL` ile öldürülür |
| 07 | Idempotent consumer | Aynı mesaj art arda, eşzamanlı ve yarıda kalarak gelir; Redis dedup'ın kaybı |
| 08 | İş seviyesinde idempotency | Idempotency-Key, farklı payload ile conflict |
| 09 | Rebalance duplicate | Uzun iş, heartbeat yok; consumer gruptan düşer, iş tekrar başlar |
| 10 | Retry topic ve DLQ | Poison, flaky, non-retriable mesajlar; ana akış bloklanmamalı |
| 11 | Transactional outbox | Dual-write, relay crash, iki relay ve aggregate sırası |
| 12 | Kafka transaction / EOS | Commit öncesi `SIGKILL`; `SIGSTOP` ile zombie ve fencing |
| 13 | Partition içi paralellik | Key bazlı seri + eşzamanlılık sınırı + kesintisiz offset öneki |
| 14 | Backpressure ve lag | Downstream kesintisi, `pause`/`resume`, lag ölçümü |
| 15 | NestJS | `-server` postfix'i, gereksiz client grubu, key'siz emit, yutulan hata |
| 16 | Log compaction | Gerçek log cleaner çalışır; tombstone, offset boşlukları, snapshot okuma |
| 17 | Graceful shutdown | Pod'a `SIGTERM`: yarım iş, LeaveGroup, kapanış süre sınırı; `SIGKILL` ile karşılaştırma |
| 18 | Partition artırma | Canlı trafik altında 3→6 partition; key taşınması, bayat metadata, drain |

Önerilen sıra numara sırası; 07'den sonra 08, 11 ve 12 birbirini tamamlıyor. 16-18 operasyon tarafı: 16 için 01 ve 14, 17 için 06 ve 09, 18 için 01, 14 ve 16 ön bilgi.

## Nasıl çalışmalı

Her egzersiz için aynı döngü:

1. **Tahmin et.** `README.md`'yi oku, `exercise.ts`'e bak, testi çalıştırmadan önce neyin fail edeceğini ve nedenini bir cümleyle yaz.
2. **Kırmızıyı gör.** `npm run ex NN`. Hata mesajlarını tahmininle karşılaştır. Yanıldıysan asıl öğrenme burada.
3. **Düzelt.** Önce en küçük değişiklikle bir testi yeşile çevir. Çoğu egzersizde testler katman katman: bir tuzağı çözünce sıradaki ortaya çıkıyor.
4. **Kanıtla.** Geçti diye bırakma; düzeltmeni geri alıp testin gerçekten o yüzden kırıldığını gör. Kafka UI'da (http://localhost:18080) topic'lere, offset'lere ve consumer group'lara bak. `KAFKA_LOG_LEVEL=INFO` ile rebalance'ları izle.
5. **Sonra karşılaştır.** `solution.ts`'i ancak testlerin geçince (ya da en az 30-40 dk takılınca) aç. Farklı bir çözüm bulduysan `attempt2.ts` olarak sakla ve `IMPL=attempt2 npm run ex NN` ile çalıştır.
6. **"Düşün" sorularını yazılı cevapla.** Egzersiz klasörüne bir `NOTES.md` aç. Soruların çoğu gerçek hayattaki tasarım kararları; mülakatlarda da tam bunlar soruluyor.

`solution.ts` ve test dosyaları proje içinde duruyor. Testleri okumak serbest ama önce sadece hata mesajlarıyla ilerlemeyi dene: production'da da elinde sadece belirtiler olacak.


## Yardımcılar (`src/lib`)

- `kafka.ts`: `createKafka({ chaos?, ...KafkaConfig })`
- `chaos-socket.ts`: kafkajs `socketFactory` ile istek seviyesinde ağ hatası. `swallowRequests(ApiKey.Produce)` isteği hiç göndermez; `dropResponses(ApiKey.Produce)` isteği gönderir, yanıtı yutar ve bağlantıyı koparır.
- `brokers.ts`: `brokers.kill/stop/start/pause/unpause(id)`, `waitForHealthy`
- `worker.ts`: `spawnWorker` (ayrı Node process), `crashNow()`, `report()`
- `messages.ts`: `readAll(kafka, topic, { expected, readUncommitted })`, `produce`
- `db.ts`: `createPool`, `withTransaction`

## Sorun giderme

- **Testler "cluster sağlıklı hale gelmedi" diyor:** Yarıda kalan bir chaos testi broker'ı durdurmuş ya da dondurmuş olabilir. `docker compose unpause kafka-1 kafka-2 kafka-3; docker compose start`
- **Log görmek istiyorum:** `KAFKA_LOG_LEVEL=INFO npm run ex 04`
- **Sıfırdan başlamak:** `npm run infra:reset`
