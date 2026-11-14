import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__tests__/failover/**/*.stress.test.ts'],
    exclude: ['node_modules/**', 'dist/**'],
    testTimeout: 3_600_000,
    hookTimeout: 240_000,
    passWithNoTests: false,
    pool: 'forks',
    fileParallelism: false,
  },
})
