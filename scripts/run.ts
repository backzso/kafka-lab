import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { implFileName } from '../src/lib/impl.js';

const arg = process.argv[2];
if (!arg) {
  console.error('Kullanım: npm run ex <no>    (ör. npm run ex 06)');
  process.exit(1);
}

const prefix = arg.padStart(2, '0');
const dir = readdirSync('exercises').find((d) => d.startsWith(`${prefix}-`));
if (!dir) {
  console.error(`exercises/${prefix}-* bulunamadı`);
  process.exit(1);
}

console.log(`\n> exercises/${dir} (${implFileName()})\n`);

const result = spawnSync('npx', ['vitest', 'run', `exercises/${dir}`, ...process.argv.slice(3)], {
  stdio: 'inherit',
  env: process.env,
});
process.exit(result.status ?? 1);
