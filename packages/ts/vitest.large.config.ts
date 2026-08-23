import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__tests__/large/**/*.test.ts'],
    env: { SQLITE_USE_URI: '1' },
    exclude: ['node_modules/**', 'dist/**'],
    testTimeout: 900_000,
    hookTimeout: 60_000,
    passWithNoTests: false,
    pool: 'forks',
    fileParallelism: false,
  },
})
