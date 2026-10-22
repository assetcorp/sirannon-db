import type { Config } from './config.ts'
import type { Driver } from './drivers/driver.ts'
import type { EngineCpuSource } from './engine-cpu.ts'
import type { RunOp } from './loadgen.ts'
import { measurePasses, type RatePasses, selectOperatingPoint, summarizeRate } from './measure.ts'
import { progress } from './progress.ts'

export interface LevelSweep {
  level: number
  ceilingOps: number
  measured: RatePasses[]
  stoppedEarly: boolean
}

export interface LevelRecord {
  concurrency: number
  client_connections: number
  runs_per_rate: number
  operating_point: Record<string, unknown>
  sweep: Record<string, unknown>[]
  sweep_stopped_early: boolean
}

export async function sweepLevel(
  driver: Driver,
  makeOp: () => RunOp,
  workloadName: string,
  level: number,
  ceilingOps: number,
  config: Config,
  cpuSource: EngineCpuSource,
): Promise<LevelSweep> {
  await driver.setConcurrency(level)
  const measured: RatePasses[] = []
  let stoppedEarly = false
  let stepsPastSaturation = -1
  for (const rate of config.targetRates) {
    progress(`sweep ${workloadName} at ${level} in flight: measuring ${rate.toLocaleString('en-US')}/s`)
    const passes = await measurePasses(makeOp, rate, 1, level, config, cpuSource)
    measured.push(passes)
    const point = summarizeRate(passes, level, config, ceilingOps)
    const p99 = (point.latency_ms as { p99: number }).p99
    progress(
      `sweep ${workloadName} at ${level} in flight: ${rate.toLocaleString('en-US')}/s ${point.sustained === true ? 'sustained' : 'not sustained'}, p99 ${p99.toFixed(1)}ms`,
    )
    if (config.sweepStopSteps < 0) {
      continue
    }
    if (point.sustained === true) {
      stepsPastSaturation = -1
      continue
    }
    stepsPastSaturation = stepsPastSaturation < 0 ? 0 : stepsPastSaturation + 1
    if (stepsPastSaturation >= config.sweepStopSteps) {
      stoppedEarly = rate !== config.targetRates[config.targetRates.length - 1]
      break
    }
  }
  return { level, ceilingOps, measured, stoppedEarly }
}

export async function repeatLevel(
  driver: Driver,
  makeOp: () => RunOp,
  workloadName: string,
  sweep: LevelSweep,
  config: Config,
  cpuSource: EngineCpuSource,
): Promise<LevelSweep> {
  const extraRuns = config.runs - 1
  if (extraRuns <= 0) {
    return sweep
  }
  await driver.setConcurrency(sweep.level)
  const measured: RatePasses[] = []
  for (const previous of sweep.measured) {
    progress(
      `repeat ${workloadName} at ${sweep.level} in flight: ${previous.targetRate.toLocaleString('en-US')}/s (${extraRuns} more passes)`,
    )
    const more = await measurePasses(makeOp, previous.targetRate, extraRuns, sweep.level, config, cpuSource)
    measured.push({
      targetRate: previous.targetRate,
      passes: [...previous.passes, ...more.passes],
      cores: [...previous.cores, ...more.cores],
    })
  }
  return { ...sweep, measured }
}

export function levelRecord(sweep: LevelSweep, driver: Driver, config: Config): LevelRecord {
  const rates = sweep.measured.map(passes => summarizeRate(passes, sweep.level, config, sweep.ceilingOps))
  return {
    concurrency: sweep.level,
    client_connections: driver.connectionsAt(sweep.level),
    runs_per_rate: sweep.measured[0]?.passes.length ?? 0,
    operating_point: selectOperatingPoint(rates, config.sloP99Ms),
    sweep: rates,
    sweep_stopped_early: sweep.stoppedEarly,
  }
}

function medianOps(point: Record<string, unknown>): number {
  const throughput = point.throughput as { median_ops?: unknown } | undefined
  return typeof throughput?.median_ops === 'number' ? throughput.median_ops : 0
}

function outranks(candidate: Record<string, unknown>, best: Record<string, unknown>): boolean {
  if (candidate.under_slo !== best.under_slo) {
    return candidate.under_slo === true
  }
  const candidateRate = candidate.target_rate as number
  const bestRate = best.target_rate as number
  if (candidateRate !== bestRate) {
    return candidateRate > bestRate
  }
  return medianOps(candidate) > medianOps(best)
}

export function peakLevelIndex(records: LevelRecord[]): number {
  let peak = 0
  for (let index = 1; index < records.length; index++) {
    const candidate = records[index]
    const best = records[peak]
    if (candidate !== undefined && best !== undefined && outranks(candidate.operating_point, best.operating_point)) {
      peak = index
    }
  }
  return peak
}
