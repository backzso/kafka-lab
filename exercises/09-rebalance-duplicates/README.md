# 09 - Rebalance kaynaklı duplicate

## Konu
Group coordinator bir üyenin hayatta olduğunu **heartbeat**'lerden anlar. `sessionTimeout` boyunca heartbeat gelmezse üye gruptan atılır ve partition'ları yeniden dağıtılır. Partition'ı alan üye (başka bir consumer ya da gruba yeniden katılan aynı consumer) **son commit edilen offset'ten** başlar. Eski üyenin iş bitince yaptığı commit reddedilir (`REBALANCE_IN_PROGRESS` / `UNKNOWN_MEMBER_ID`).

Sonuç: aynı iş iki kez çalışır. E-posta iki kez gider, rapor iki kez üretilir. İş hep timeout'tan uzun sürüyorsa bu sonsuz bir döngüye dönüşebilir.

## kafkajs'e özgü davranış
- Java client'ta heartbeat arka plan thread'inden gider; takılan işleri `max.poll.interval.ms` yakalar.
- kafkajs'te arka plan heartbeat thread'i **yok**. Heartbeat şu durumlarda gider:
  - mesajlar/batch'ler arasında,
  - senin çağırdığın `heartbeat()` ile,
  - ve **boş dönen fetch'lerden sonra**.
- kafkajs cluster'daki her broker için ayrı bir fetcher çalıştırır. Consumer'ın partition'ı olmayan broker'ların fetcher'ı boş fetch yapar ve heartbeat gönderir. Bu seni **tesadüfen** korur.
- Tüm broker'larda meşgul partition'ın olduğunda (yük altında gerçekçi bir durum) bu koruma kalkar. Test tam olarak bu durumu kuruyor: 3 partition, leader'ları 3 farklı broker'da.

## Senaryo
`sessionTimeout: 3000`, her iş ~4.5 sn. Test her işin **kaç kez başladığını** sayıyor.

## Görev (`exercise.ts`)
`startJobWorker`: Uzun süren işler sırasında consumer gruptan atılmasın; her iş tam bir kez çalışsın. İş bitince (başarılı ya da hatalı) arkada çalışan bir şey kalmasın.

## İpuçları
- `eachMessage` payload'unda `heartbeat()` var.
- Tek bir uzun `await` sırasında periyodik iş: `setInterval` + `finally { clearInterval }`.
- Rebalance sırasında `heartbeat()` **hata fırlatır**. Interval içindeki yakalanmayan bir rejection process'i düşürebilir.

## Düşün
- `sessionTimeout`'u 5 dakikaya çıkarmak neden kötü bir çözüm? (Gerçekten ölen bir consumer 5 dakika fark edilmez.)
- İş 30 dakika sürüyorsa ne yaparsın? İşi başka bir job sistemine devredebilir ya da `pause()` edip işi arka planda çalıştırıp bitince `resume()` edebilirsin.
- Heartbeat rebalance hatası fırlatınca devam eden işi iptal etmeli misin? Partition artık başkasında ve iş orada tekrar başlayacak. İdempotency (07) burada da son savunma hattı.
