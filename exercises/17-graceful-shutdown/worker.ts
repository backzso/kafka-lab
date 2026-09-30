import { createKafka } from '../../src/lib/kafka.js';
import { loadImpl } from '../../src/lib/impl.js';
import { report } from '../../src/lib/worker.js';
import { sleep } from '../../src/lib/wait.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);
const { TOPIC, GROUP, NAME } = process.env as Record<string, string>;

await impl.runService({
  kafka: createKafka({ clientId: NAME }),
  topic: TOPIC,
  consumerConfig: { groupId: GROUP, sessionTimeout: 15_000, heartbeatInterval: 1000, maxWaitTimeInMs: 200 },
  shutdownTimeoutMs: 3000,
  handle: async (message, partition) => {
    const value = message.value!.toString();
    await report({ type: 'start', value, partition });
    // Asla bitmeyen iş: ör. timeout'u olmayan bir HTTP çağrısı.
    if (value === 'stuck') await new Promise(() => undefined);
    await sleep(300);
    await report({ type: 'done', value, partition });
  },
});
await report({ type: 'ready' });
