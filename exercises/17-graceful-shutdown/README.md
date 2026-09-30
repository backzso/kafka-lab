# 17 - Graceful shutdown ve rebalance

## Konu
Kubernetes'te bir deploy şöyle ilerler: yeni pod ayağa kalkar, eski pod **SIGTERM** alır. Eski pod `terminationGracePeriodSeconds` (varsayılan 30 sn) içinde çıkmazsa **SIGKILL** gelir.

Node.js'te SIGTERM için handler yoksa process **anında** ölür. Kafka açısından bu bir crash'tir:
- Elindeki mesaj yarıda kalır, offset'i commit edilmez, mesaj tekrar işlenir (06, 09).
- Coordinator üyenin öldüğünü bilmez. `sessionTimeout` dolana kadar heartbeat bekler. O süre boyunca pod'un partition'larını **kimse okumaz**.
- Rolling deploy'da bu her pod için tekrarlanır: 10 pod × 30 sn kesinti.

Temiz kapanış sırası:
1. Yeni mesaj almayı bırak.
2. Elindeki mesajı bitir.
3. Offset'leri commit et.
4. **LeaveGroup** gönder. Coordinator beklemeden rebalance başlatır ve partition'lar saniyeler içinde diğer pod'lara geçer.
5. Çık.

kafkajs'te `consumer.disconnect()` 1-4'ü yapar. Ama "elindeki mesajı bitir" adımı sonsuza kadar sürebilir, örneğin timeout'u olmayan bir HTTP çağrısı. O yüzden kapanışın bir **üst süre sınırı** olmalı.

### Rebalance neden pahalı? (kavramsal)
kafkajs **eager** rebalance kullanır: bir üye çıkınca ya da katılınca grubun **tüm** üyeleri partition'larını bırakır ve yeniden atama bekler. Buna "stop-the-world" denir.

Java client ve librdkafka'da bunu hafifleten özellikler var. kafkajs bunları **desteklemiyor**:
- **Static membership** (`group.instance.id`): Pod kalıcı bir kimlikle katılır. Restart edip `sessionTimeout` dolmadan geri dönerse rebalance hiç olmaz, aynı partition'ları geri alır. Kubernetes StatefulSet ile sık kullanılır. Bunun için pod kapanırken LeaveGroup **göndermez**.
- **Cooperative (incremental) rebalance**: Sadece yer değiştiren partition'lar bırakılır, diğerleri işlenmeye devam eder.
- **KIP-848** (Kafka 4.0): Yeni consumer group protokolü. Atamayı broker yapar, rebalance'lar artımlı olur.

## Senaryo
Test servisini ayrı Node process'lerinde (`worker.ts`) çalıştırır. `sessionTimeout` 15 sn, her mesaj 300 ms sürer.
1. İki pod aynı grupta, 4 partition. pod-a mesaj işlerken SIGTERM alır. Beklenen: 10 sn içinde **0 koduyla** çıkması, elindeki işi bitirmesi, pod-b'nin partition'ları **5 sn'den kısa sürede** devralması, kayıp ve tekrar olmaması.
2. Tek pod, bitmeyen bir iş işlerken SIGTERM alır. Beklenen: `shutdownTimeoutMs` (3 sn) sonra **sıfır olmayan kodla** kendi çıkması.
3. Bilgi amaçlı (her zaman geçer): pod-a SIGKILL ile ölür. Devralmanın ne kadar sürdüğünü karşılaştır.

## Görev (`exercise.ts`)
`runService`: Consumer'ı başlat ve process'in yaşam döngüsünü yönet. SIGTERM (ve SIGINT) gelince temiz kapan. Kapanış `shutdownTimeoutMs` içinde bitmezse hata koduyla çık.

## İpuçları
- `process.once('SIGTERM', handler)`. Handler kurulunca Node varsayılan "hemen öl" davranışını bırakır. Çıkışı artık **sen** yapmalısın (`process.exit`).
- `consumer.disconnect()` elindeki `eachMessage`'ın bitmesini bekler.
- Süre sınırı için `setTimeout(...)`. `timer.unref()` normal çıkışı geciktirmez.
- İkinci bir SIGTERM ya da SIGINT (Ctrl+C'ye iki kez basmak) kapanışı ikinci kez başlatmamalı.

## Düşün
- Handler'ı `consumer.run()`'dan **önce** mi sonra mı kurmalısın? Açılış sırasında SIGTERM gelirse ne olur?
- Kubernetes pod'u kapatırken önce Service endpoint'lerinden çıkarır, sonra SIGTERM gönderir, ama bunlar eşzamanlı olmayabilir. HTTP servislerinde bu yüzden `preStop: sleep 5` kullanılır. Kafka consumer'ında buna gerek var mı?
- Producer da kullanan bir serviste kapanış sırası ne olmalı? (Önce consumer: yeni iş gelmesin. Sonra producer: içerideki gönderimler flush edilsin.)
- `shutdownTimeoutMs` neden `terminationGracePeriodSeconds`'tan kısa olmalı?
- Static membership varken pod kapanırken LeaveGroup göndermemek neden mantıklı? Ya pod gerçekten kalıcı olarak kaldırılıyorsa (scale down)?
