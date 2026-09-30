# 06 - Offset commit semantiği

## Konu
Consumer'ın "nerede kaldım" bilgisi commit edilen offset'tir. **Commit'in işlemeye göre nerede yapıldığı** teslim garantisini belirler:

| Sıra | Crash anı | Sonuç |
|------|-----------|-------|
| işle → commit | işledikten sonra, commit'ten önce | mesaj **tekrar işlenir** (at-least-once) |
| commit → işle | commit'ten sonra, işlemeden önce | mesaj **hiç işlenmez** (at-most-once) |

kafkajs varsayılanı (`autoCommit: true`): offset, handler bittikten sonra "resolve" edilir. Commit ise **periyodik** yapılır: batch sonunda, ya da `autoCommitInterval` / `autoCommitThreshold` dolunca. Crash olursa son commit'ten sonra işlenen **her şey** tekrar gelir. Batch'te 500 mesaj varsa 500 duplicate demektir.

Kafka'da commit edilen offset **bir sonraki okunacak** mesajın offset'idir: `message.offset + 1`.

## Senaryo (gerçek crash)
Test, consumer'ı ayrı bir Node process'inde (`worker.ts`) çalıştırıyor. Consumer, `v5` mesajını işlemeye başlarken `SIGKILL` ile ölüyor: ne `finally`, ne `disconnect`, ne de commit çalışıyor. Aynı grupta ikinci bir process başlatılıp kalan mesajlar işleniyor.

## Görev (`exercise.ts`)
1. `runAtLeastOnce`: Hiçbir mesaj kaybolmasın. Crash'ten önce **tamamlanmış** mesajlar tekrar işlenmesin; yani duplicate penceresi en fazla 1 mesaj olsun.
2. `runAtMostOnce`: Hiçbir mesaj iki kez işlenmesin (kayıp kabul).

## İpuçları
- `consumer.run({ autoCommit: false, eachMessage })`
- `consumer.commitOffsets([{ topic, partition, offset }])`
- Offset string'dir; `(BigInt(offset) + 1n).toString()`

## Düşün
- Her mesaj için senkron commit pahalıdır (her biri broker'a bir istek). `eachBatch` + `resolveOffset` + `commitOffsetsIfNecessary` ile nasıl dengelersin?
- at-least-once + idempotent işleme = etkin şekilde exactly-once (bkz. 07)
