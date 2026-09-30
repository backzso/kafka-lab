export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export interface WaitForOptions {
  timeoutMs?: number;
  intervalMs?: number;
  message?: string;
}

/** predicate true dönene kadar bekler; süre dolarsa hata fırlatır. predicate'in fırlattığı hatalar yutulur. */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  { timeoutMs = 10_000, intervalMs = 100, message = 'waitFor zaman aşımı' }: WaitForOptions = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (e) {
      lastError = e;
    }
    await sleep(intervalMs);
  }
  const suffix = lastError instanceof Error ? ` (son hata: ${lastError.message})` : '';
  throw new Error(`${message} (${timeoutMs}ms)${suffix}`);
}
