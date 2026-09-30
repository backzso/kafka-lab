import { createKafka } from '../../src/lib/kafka.js';
import { loadImpl } from '../../src/lib/impl.js';
import { crashNow, report } from '../../src/lib/worker.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);
const { MODE, TOPIC, GROUP, CRASH_AT } = process.env as Record<string, string>;

const run = MODE === 'atMostOnce' ? impl.runAtMostOnce : impl.runAtLeastOnce;

await run({
  kafka: createKafka(),
  topic: TOPIC,
  consumerConfig: { groupId: GROUP, sessionTimeout: 6000, heartbeatInterval: 1000, maxWaitTimeInMs: 200 },
  handle: async (message) => {
    const value = message.value!.toString();
    if (value === CRASH_AT) crashNow();
    await report({ type: 'processed', value });
  },
});
await report({ type: 'ready' });
