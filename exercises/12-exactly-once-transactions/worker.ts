import { createKafka } from '../../src/lib/kafka.js';
import { loadImpl } from '../../src/lib/impl.js';
import { crashNow, report } from '../../src/lib/worker.js';

const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);
const { INPUT, OUTPUT, GROUP, TXN_ID, FAULT, FAULT_AT } = process.env as Record<string, string>;

let faultFired = false;

await impl.startProcessor({
  kafka: createKafka(),
  inputTopic: INPUT,
  outputTopic: OUTPUT,
  consumerConfig: { groupId: GROUP, sessionTimeout: 6000, heartbeatInterval: 500, maxWaitTimeInMs: 200 },
  transactionalId: TXN_ID,
  transform: (v) => v.toUpperCase(),
  faults: {
    beforeCommit: async (message) => {
      const value = message.value!.toString();
      await report({ type: 'beforeCommit', value });
      if (faultFired || value !== FAULT_AT) return;
      faultFired = true;
      if (FAULT === 'crash') crashNow();
      if (FAULT === 'freeze') {
        await report({ type: 'freezing', value });
        // Uzun bir GC pause / VM askıya alınması: process tamamen durur, test SIGCONT ile uyandırır.
        process.kill(process.pid, 'SIGSTOP');
        await report({ type: 'resumed', value });
      }
    },
  },
  onFenced: (e) => void report({ type: 'fenced', error: e.message }),
});
await report({ type: 'ready' });
