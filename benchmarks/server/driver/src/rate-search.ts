import type { Config } from './config.ts'
import type { Driver } from './drivers/driver.ts'
import type { EngineCpuSource } from './engine-cpu.ts'
import type { LevelSweep } from './levels.ts'
import type { RunOp } from './loadgen.ts'
import { heldWithinSlo, measurePasses, type RatePasses, summarizeRate } from './measure.ts'
import { progress } from './progress.ts'

export const RATE_SEARCH_MIN_GAP_RATIO = 2

function ascendingRates(config: Config): number[] {
  return [...new Set(config.targetRates)].sort((a, b) => a - b)
}

export function ladderHasWideGap(config: Config): boolean {
  const rates = ascendingRates(config)
  return rates.some((rate, index) => index > 0 && rate > RATE_SEARCH_MIN_GAP_RATIO * (rates[index - 1] as number))
}

function searchBounds(sweep: LevelSweep, config: Config): { held: number; missed: number } | null {
  const points = sweep.measured.map(passes => summarizeRate(passes, sweep.level, config, sweep.ceilingOps))
  const heldRates = points
    .filter(point => heldWithinSlo(point, config.sloP99Ms))
    .map(point => point.target_rate as number)
  if (heldRates.length === 0) {
    return null
  }
  const held = Math.max(...heldRates)
  const missed = ascendingRates(config).find(rate => rate > held)
  if (missed === undefined || missed <= RATE_SEARCH_MIN_GAP_RATIO * held) {
    return null
  }
  if (!sweep.measured.some(passes => passes.targetRate === missed)) {
    return null
  }
  return { held, missed }
}

export async function searchRateGap(
  driver: Driver,
  makeOp: () => RunOp,
  workloadName: string,
  sweep: LevelSweep,
  config: Config,
  cpuSource: EngineCpuSource,
): Promise<LevelSweep> {
  if (config.rateSearchSteps <= 0) {
    return sweep
  }
  const bounds = searchBounds(sweep, config)
  if (bounds === null) {
    return sweep
  }
  await driver.setConcurrency(sweep.level)
  let low = bounds.held
  let high = bounds.missed
  const probes: RatePasses[] = []
  for (let step = 0; step < config.rateSearchSteps; step++) {
    const rate = Math.round((low + high) / 2)
    if (rate <= low || rate >= high) {
      break
    }
    progress(
      `search ${workloadName} at ${sweep.level} in flight: measuring ${rate.toLocaleString('en-US')}/s ` +
        `between ${low.toLocaleString('en-US')}/s and ${high.toLocaleString('en-US')}/s (${config.runs} passes)`,
    )
    const measured = await measurePasses(makeOp, rate, config.runs, sweep.level, config, cpuSource)
    const probe: RatePasses = { ...measured, searched: true }
    probes.push(probe)
    const point = summarizeRate(probe, sweep.level, config, sweep.ceilingOps)
    const held = heldWithinSlo(point, config.sloP99Ms)
    progress(
      `search ${workloadName} at ${sweep.level} in flight: ${rate.toLocaleString('en-US')}/s ` +
        `${held ? 'held' : 'missed'} the p99 target, p99 ${(point.latency_ms as { p99: number }).p99.toFixed(1)}ms`,
    )
    if (held) {
      low = rate
    } else {
      high = rate
    }
  }
  const measured = [...sweep.measured, ...probes].sort((a, b) => a.targetRate - b.targetRate)
  return { ...sweep, measured }
}
