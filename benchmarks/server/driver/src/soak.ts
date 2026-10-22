import type { Config } from './config.ts'
import { type EngineCpuSource, engineCpuBlock, withEngineCores } from './engine-cpu.ts'
import { summarizeFailures } from './failures.ts'
import { type LoadResult, type RunOp, runOpenLoop } from './loadgen.ts'
import { ERROR_RATE_CEILING } from './measure.ts'
import { progress } from './progress.ts'

const SOAK_BUCKET_SECONDS = 30

export async function runSoak(
  makeOp: () => RunOp,
  operatingPoint: Record<string, unknown>,
  level: number,
  config: Config,
  cpuSource: EngineCpuSource,
  workloadName: string,
): Promise<Record<string, unknown>> {
  if (operatingPoint.sustained !== true) {
    return {
      skipped: true,
      reason: 'the sweep found no rate this engine sustained, so there is no operating point to hold',
    }
  }
  const targetRate = operatingPoint.target_rate as number
  progress(
    `soak ${workloadName}: holding ${targetRate.toLocaleString('en-US')}/s at ${level} in flight for ${config.soakSeconds}s`,
  )
  const soakStart = performance.now()
  const ticker = setInterval(() => {
    const elapsed = Math.round((performance.now() - soakStart) / 1000)
    progress(`soak ${workloadName}: ${elapsed}s of ${Math.round(config.warmupSeconds + config.soakSeconds)}s`)
  }, 60_000)
  let pass: LoadResult
  let soakCores: number | null
  try {
    const measured = await withEngineCores(cpuSource, () =>
      runOpenLoop(makeOp(), targetRate, config.warmupSeconds, config.soakSeconds, level, SOAK_BUCKET_SECONDS),
    )
    pass = measured.result
    soakCores = measured.coresUsed
  } finally {
    clearInterval(ticker)
  }
  progress(
    `soak ${workloadName}: done, achieved ${Math.round(pass.achievedRate).toLocaleString('en-US')}/s, p99 ${pass.p99Ms.toFixed(1)}ms`,
  )
  const windows = (pass.buckets ?? []).map(bucket => ({
    start_seconds: bucket.startSeconds,
    achieved_rate: bucket.achievedRate,
    errors: bucket.errors,
    p50_ms: bucket.p50Ms,
    p99_ms: bucket.p99Ms,
    max_ms: bucket.maxMs,
  }))
  const worstWindow = windows.reduce(
    (worst: (typeof windows)[number] | null, window) =>
      worst === null || window.p99_ms > worst.p99_ms ? window : worst,
    null,
  )
  return {
    skipped: false,
    target_rate: targetRate,
    concurrency: level,
    seconds: config.soakSeconds,
    window_seconds: SOAK_BUCKET_SECONDS,
    achieved_rate: pass.achievedRate,
    held: pass.sustained && pass.errorRate < ERROR_RATE_CEILING,
    p99_under_slo: pass.p99Ms <= config.sloP99Ms,
    error_rate: pass.errorRate,
    latency_ms: {
      p50: pass.p50Ms,
      p95: pass.p95Ms,
      p99: pass.p99Ms,
      p999: pass.p999Ms,
      max: pass.maxMs,
      mean: pass.meanMs,
    },
    worst_window: worstWindow,
    windows,
    generator: { offered_fraction: pass.offeredFraction, cap_occupancy: pass.capOccupancy },
    engine_cpu: engineCpuBlock([soakCores], config.engineCpus),
    failures: summarizeFailures([pass.failures]),
  }
}
