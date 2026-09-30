import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

/**
 * IMPL ortam değişkeni hangi dosyanın test edileceğini seçer:
 * - boş: exercise.ts (senin kodun)
 * - "solution": solution.ts
 * - başka bir ad, ör. "attempt2": attempt2.ts (alternatif denemelerini karşılaştırmak için)
 */
export function implFileName(): string {
  const impl = process.env.IMPL;
  if (!impl) return 'exercise.ts';
  return impl.endsWith('.ts') ? impl : `${impl}.ts`;
}

export const usingSolution = () => process.env.IMPL === 'solution';

/**
 * Test dosyasının yanındaki implementasyonu yükler.
 * Kullanım: `const impl = await loadImpl<typeof import('./exercise.js')>(import.meta.url);`
 */
export async function loadImpl<T>(callerUrl: string): Promise<T> {
  const dir = path.dirname(fileURLToPath(callerUrl));
  return (await import(pathToFileURL(path.join(dir, implFileName())).href)) as T;
}
