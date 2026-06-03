import { defineConfig } from 'vitest/config'

const DEFAULT_DURATION_MS = 120_000
const TEST_TIMEOUT_PADDING_MS = 60_000

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') {
    return fallback
  }

  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return parsed
}

const durationMs = readPositiveInteger('SIRANNON_SOAK_DURATION_MS', DEFAULT_DURATION_MS)
const testTimeout = readPositiveInteger('SIRANNON_SOAK_TEST_TIMEOUT_MS', durationMs + TEST_TIMEOUT_PADDING_MS)

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__tests__/e2e/**/*.soak.test.ts'],
    exclude: ['node_modules/**', 'dist/**'],
    testTimeout,
    hookTimeout: 30_000,
    passWithNoTests: false,
    pool: 'forks',
    fileParallelism: false,
  },
})
