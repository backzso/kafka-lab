# 12 - Kafka transaction ve exactly-once (EOS)

## Konu
Kafka → işle → Kafka akışlarında (stream processing) Kafka gerçek exactly-once sağlayabilir. Gereken tek şey şu üç işlemi **tek bir transaction**'da yapmak:
1. Output mesajlarını yazmak
2. Input offset'ini commit etmek (`sendOffsets`)
3. Commit / abort

```
txn = producer.transaction()
txn.send(output)
txn.sendOffsets({ consumerGroupId, topics: [{ topic, partitions: [{ partition, offset: next }] }] })
txn.commit()
```

Crash olursa transaction commit edilmez: output abort edilir ve offset ilerlemez. Yeni instance aynı input'u tekrar işler ve bu sefer commit eder.

**İki kritik detay:**
- **Downstream `read_committed` okumalı.** Abort edilen kayıtlar log'da fiziksel olarak durur; `read_uncommitted` consumer onları görür.
- **`transactionalId` kalıcı olmalı.** Yeni instance aynı id ile `initTransactions` yaptığında:
  - eski instance'ın açık transaction'ı abort edilir,
  - epoch artar ve eski producer **fence** edilir (`PRODUCER_FENCED`).
  - Id her instance'ta rastgele olursa ölen instance'ın açık transaction'ı `transactionTimeout` dolana kadar (varsayılan 60sn) açık kalır. `read_committed` consumer'lar bu süre boyunca o noktadan ileri okuyamaz (LSO takılır). Bu lab'de denendi: 8 olaydan sadece 3'ü görüldü.

**Zombie:** GC pause veya VM askıya alınması yüzünden donan bir instance gruptan düşer. Yerine yenisi gelir. Eski instance uyanıp elindeki işi commit etmeye çalışır; fence edilmezse duplicate üretir.

## Senaryolar (gerçek process'ler)
1. **Crash:** Worker `event-3` için commit'ten hemen önce `SIGKILL` ile ölüyor. Yeni worker devam ediyor.
2. **Zombie:** Worker `event-2`'de `SIGSTOP` ile donuyor. Yerine yenisi geliyor ve her şeyi işliyor. Sonra eski worker `SIGCONT` ile uyanıyor.

## Görev (`exercise.ts`)
`startProcessor`'ı transaction kullanacak şekilde yeniden yaz. Fence edildiğinde `onFenced`'ı çağır ve dur: retry etme, yeni producer açma.

## İpuçları
- `kafka.producer({ transactionalId, idempotent: true, maxInFlightRequests: 1 })`
- `consumer.run({ autoCommit: false, ... })`: offset'i transaction commit eder.
- Fence hatası `KafkaJSProtocolError` olarak gelir; `type` alanına bak (gerekirse `cause` zincirinde ara, bkz. 03).

## Düşün
- Input'ta birden fazla partition varsa tek bir `transactionalId` neden yetmez? (kafkajs KIP-447'yi desteklemiyor, Java client ise `groupMetadata` ile fencing yapabiliyor. kafkajs'te klasik yöntem: input partition başına bir `transactionalId`.)
- EOS sadece Kafka içinde geçerli. Output bir DB'ye ya da e-postaya gidiyorsa ne olur? (07, 11)
- Transaction'lar throughput'u düşürür. Her mesaj yerine batch başına bir transaction nasıl yaparsın?
