import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__tests__/failover/**/*.test.ts'],
    exclude: ['node_modules/**', 'dist/**'],
    testTimeout: 120_000,
    hookTimeout: 60_000,
    passWithNoTests: false,
    pool: 'forks',
    fileParallelism: false,
  },
})
