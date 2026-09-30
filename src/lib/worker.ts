import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { waitFor } from './wait.js';

export interface WorkerMessage {
  type: string;
  [key: string]: unknown;
}

export interface WorkerHandle {
  messages: WorkerMessage[];
  /** Process çıkınca resolve olur (kill dahil). */
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  waitForMessage(predicate: (m: WorkerMessage) => boolean, timeoutMs?: number): Promise<WorkerMessage>;
  kill(signal?: NodeJS.Signals): void;
  child: ChildProcess;
}

/**
 * Bir TypeScript dosyasını ayrı Node process'inde çalıştırır. Crash senaryoları (SIGKILL) için kullanılır.
 * Worker içinden `report()` ile test'e mesaj gönderilir.
 */
export function spawnWorker(scriptUrl: string | URL, env: Record<string, string> = {}): WorkerHandle {
  const child = fork(fileURLToPath(scriptUrl), [], {
    execArgv: ['--import', 'tsx'],
    env: { ...process.env, ...env },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
  const messages: WorkerMessage[] = [];
  child.on('message', (m) => messages.push(m as WorkerMessage));
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.on('exit', (code, signal) => resolve({ code, signal }));
  });

  return {
    messages,
    exited,
    child,
    async waitForMessage(predicate, timeoutMs = 30_000) {
      let found: WorkerMessage | undefined;
      await waitFor(() => {
        found = messages.find(predicate);
        if (!found && child.exitCode !== null) throw new Error(`worker ${child.exitCode} koduyla çıktı`);
        return !!found;
      }, { timeoutMs, message: 'worker beklenen mesajı göndermedi' });
      return found!;
    },
    kill(signal = 'SIGKILL') {
      if (child.exitCode === null && child.signalCode === null) child.kill(signal);
    },
  };
}

/** Worker içinden test process'ine mesaj gönderir; teslim edilene kadar bekler. */
export function report(message: WorkerMessage): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!process.send) return resolve();
    process.send(message, (err: Error | null) => (err ? reject(err) : resolve()));
  });
}

/** Process'i anında öldürür: finally blokları, disconnect, commit hiçbiri çalışmaz. */
export function crashNow(): never {
  process.kill(process.pid, 'SIGKILL');
  throw new Error('unreachable');
}
