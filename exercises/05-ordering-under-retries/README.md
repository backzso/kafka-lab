# 05 - Retry altında sıralama

## Konu
Aynı key = aynı partition = sıralı log. Ama bu sadece mesajlar broker'a **gönderildiği sırayla** ulaşırsa işe yarar.

**Java client:** `max.in.flight.requests.per.connection=1` (ya da `enable.idempotence=true` + ≤5) sıralamayı korur. Retry edilen batch kuyruğun başına geri konur ve arkasındakiler bekler.

**kafkajs:** Her `producer.send()` çağrısı bağımsız bir istek ve kendi retry döngüsü. Aynı key için **eşzamanlı** yapılan `send()` çağrıları arasında sıra garantisi yok:
- İlk istek zaman aşımına uğrarsa arkadakiler yazılır, retry en sona düşer.
- `maxInFlightRequests: 1` bunu çözmez: timeout olunca slot boşalır, sıradaki istek gider.
- `idempotent: true` de çözmez: sequence numarası istek oluşturulurken okunur.
- Bu lab'de ölçüldü; dört konfigürasyonun hepsinde sonuç: `["2","3","5","4","1"]`.

Gerçek hayatta bu sorun şöyle çıkar: aynı sipariş için gelen iki HTTP isteği (`PAID`, `SHIPPED`) iki ayrı `send()` çağrısı yapar.

## Görev (`exercise.ts`)
`OrderedPublisher.publish(key, value)` eşzamanlı çağrılabilir. Şunları sağla:
1. Aynı key'e ait mesajlar `publish` çağrı sırasıyla yazılsın.
2. Farklı key'ler birbirini **beklemesin**: bir key'in yavaşlığı diğerlerini durdurmasın.
3. Bir gönderim başarısız olursa hata o çağırana dönsün, ama o key'in sonraki gönderimleri takılı kalmasın.
4. Bellek sızıntısı olmasın: işi biten key'ler içeride tutulmasın.

## Senaryo
Chaos soketi ilk Produce isteğini yutuyor, istek hiç gitmiyor. Client 1.5sn sonra timeout alıyor ve retry ediyor.

## İpuçları
- Key başına bir Promise zinciri: `tail = tail.then(() => send())`
- Zincir, bir hata yüzünden reject durumda kalmamalı.

## Düşün
- Tüm mesajları tek bir `send()` içinde batch olarak göndermek neden sıralamayı korur?
- Bu çözüm tek bir process içinde çalışır. Aynı key'i iki farklı pod gönderiyorsa ne yaparsın? (İpucu: sıralamayı üreten tarafta tek sahip; outbox, bkz. 11)
