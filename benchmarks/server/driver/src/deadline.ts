import type { Config } from './config.ts'

const MIN_SEED_ROWS_PER_SEC = 20_000
const WORKLOAD_DEADLINE_SAFETY = 3
const WORKLOAD_DEADLINE_FLOOR_MS = 120_000

export function workloadDeadlineMs(config: Config): number {
  if (config.workloadTimeoutMs > 0) {
    return config.workloadTimeoutMs
  }
  const windowSeconds = config.warmupSeconds + config.measureSeconds
  const seedSeconds = config.dataSize / MIN_SEED_ROWS_PER_SEC
  const levelSweepSeconds = config.concurrencyLevels.length * config.targetRates.length * windowSeconds
  const peakRepeatSeconds = config.targetRates.length * (config.runs - 1) * windowSeconds
  const soakSeconds = config.soakSeconds > 0 ? config.warmupSeconds + config.soakSeconds : 0
  return Math.max(
    WORKLOAD_DEADLINE_FLOOR_MS,
    Math.ceil((seedSeconds + levelSweepSeconds + peakRepeatSeconds + soakSeconds) * WORKLOAD_DEADLINE_SAFETY) * 1000,
  )
}

export class WorkloadStallError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'WorkloadStallError'
  }
}

export function withDeadline<T>(work: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new WorkloadStallError(`${label} exceeded its ${Math.round(ms / 1000)}s deadline and was aborted as a stall`),
      )
    }, ms)
  })
  return Promise.race([work, deadline]).finally(() => {
    if (timer !== undefined) {
      clearTimeout(timer)
    }
  })
}
