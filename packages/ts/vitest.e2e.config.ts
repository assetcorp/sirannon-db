import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__tests__/e2e/**/*.test.ts'],
    exclude: ['node_modules/**', 'dist/**', 'src/__tests__/e2e/**/*.soak.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    passWithNoTests: false,
    pool: 'forks',
    fileParallelism: false,
  },
})
