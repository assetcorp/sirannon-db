import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/__tests__/**/*.test.ts', 'benchmarks/__tests__/**/*.test.ts'],
    testTimeout: 10_000,
    passWithNoTests: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/index.ts',
        'src/**/__tests__/**',
        'src/client/**',
        'src/core/types.ts',
        'src/core/cdc/types.ts',
        'src/core/hooks/types.ts',
        'src/core/migrations/types.ts',
      ],
      reporter: ['text', 'html', 'lcov'],
      reportsDirectory: './coverage',
      thresholds: {
        statements: 80,
        branches: 75,
        functions: 80,
        lines: 80,
      },
    },
  },
})
