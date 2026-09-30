import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['exercises/**/*.test.ts'],
    // Testler aynı cluster'ı paylaşıyor ve bazıları broker durduruyor; dosyalar sırayla koşmalı.
    fileParallelism: false,
    testTimeout: 120_000,
    hookTimeout: 120_000,
    pool: 'forks',
    globalSetup: ['src/lib/global-setup.ts'],
  },
});
