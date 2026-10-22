import { classifyPass, majorityVerdict } from './classify.ts'
import type { Config } from './config.ts'
import { type EngineCpuSource, engineCpuBlock, withEngineCores } from './engine-cpu.ts'
import { summarizeFailures } from './failures.ts'
import { type LoadResult, type RunOp, runOpenLoop } from './loadgen.ts'
import { median, summarizeMetric } from './stats.ts'

export const ERROR_RATE_CEILING = 0.01

export interface RatePasses {
  targetRate: number
  passes: LoadResult[]
  cores: Array<number | null>
}

function generatorBlock(passes: LoadResult[], maxInFlight: number): Record<string, unknown> {
  return {
    max_in_flight: maxInFlight,
    max_in_flight_observed: median(passes.map(pass => pass.maxInFlightObserved)),
    reached_cap: passes.filter(pass => pass.reachedCap).length > passes.length / 2.0,
    cap_occupancy: median(passes.map(pass => pass.capOccupancy)),
    occupancy_samples: median(passes.map(pass => pass.occupancySamples)),
    offered_rate: median(passes.map(pass => pass.offeredRate)),
    offered_fraction: median(passes.map(pass => pass.offeredFraction)),
    dispatched_in_window: median(passes.map(pass => pass.dispatchedInWindow)),
    expected_in_window: median(passes.map(pass => pass.expectedInWindow)),
    note:
      'offered_fraction is the share of the target rate the generator actually offered, and it is ' +
      'the field to read for whether the load was delivered. dispatched_in_window and ' +
      'expected_in_window always agree and are not evidence of that: the generator walks every ' +
      'scheduled request however late it is running, so the count comes out right even when the ' +
      'schedule took several times its window to deliver. Only the elapsed time shows the shortfall, ' +
      'which is what offered_rate measures. cap_occupancy is the fraction of the window, sampled off ' +
      'the request path, that the generator spent with its in-flight cap full; near one means ' +
      'requests were waiting on the server rather than on the generator, and occupancy_samples is ' +
      'how many samples that fraction rests on.',
  }
}

export async function measurePasses(
  makeOp: () => RunOp,
  targetRate: number,
  runs: number,
  level: number,
  config: Config,
  cpuSource: EngineCpuSource,
): Promise<RatePasses> {
  const passes: LoadResult[] = []
  const cores: Array<number | null> = []
  for (let run = 0; run < runs; run++) {
    const runOp = makeOp()
    const measured = await withEngineCores(cpuSource, () =>
      runOpenLoop(runOp, targetRate, config.warmupSeconds, config.measureSeconds, level),
    )
    passes.push(measured.result)
    cores.push(measured.coresUsed)
  }
  return { targetRate, passes, cores }
}

export function summarizeRate(
  measured: RatePasses,
  level: number,
  config: Config,
  ceilingOps: number,
): Record<string, unknown> {
  const { passes } = measured
  const throughputSamples = passes.map(pass => pass.achievedRate)
  const summary = summarizeMetric(throughputSamples, 0.95, config.seed)
  const classifications = passes.map(pass => classifyPass(pass, ceilingOps))
  const verdict = majorityVerdict(classifications.map(entry => entry.verdict))

  return {
    target_rate: measured.targetRate,
    concurrency: level,
    throughput: {
      median_ops: summary.median,
      mean_ops: summary.mean,
      stddev_ops: summary.stddev,
      cv: summary.cv,
      ci_low_ops: summary.ciLow,
      ci_high_ops: summary.ciHigh,
      confidence: summary.confidence,
      runs: summary.runs,
      samples: throughputSamples,
    },
    latency_ms: {
      p50: median(passes.map(pass => pass.p50Ms)),
      p95: median(passes.map(pass => pass.p95Ms)),
      p99: median(passes.map(pass => pass.p99Ms)),
      p999: median(passes.map(pass => pass.p999Ms)),
      max: median(passes.map(pass => pass.maxMs)),
      mean: median(passes.map(pass => pass.meanMs)),
    },
    generator: generatorBlock(passes, level),
    engine_cpu: engineCpuBlock(measured.cores, config.engineCpus),
    sustained: verdict === 'sustained',
    server_saturated: verdict === 'server_saturated' || verdict === 'both',
    client_bound: verdict === 'client_bound' || verdict === 'both',
    limit_verdict: verdict,
    verdict_samples: classifications.map(entry => entry.verdict),
    error_rate: median(passes.map(pass => pass.errorRate)),
    failures: summarizeFailures(passes.map(pass => pass.failures)),
  }
}

export function selectOperatingPoint(sweep: Record<string, unknown>[], sloP99Ms: number): Record<string, unknown> {
  const p99 = (rate: Record<string, unknown>): number => (rate.latency_ms as { p99: number }).p99
  const errorRate = (rate: Record<string, unknown>): number => rate.error_rate as number
  const targetRate = (rate: Record<string, unknown>): number => rate.target_rate as number

  const withinSlo = sweep.filter(
    rate => rate.sustained === true && p99(rate) <= sloP99Ms && errorRate(rate) < ERROR_RATE_CEILING,
  )
  if (withinSlo.length > 0) {
    const chosen = withinSlo.reduce((best, rate) => (targetRate(rate) > targetRate(best) ? rate : best))
    return { ...chosen, under_slo: true }
  }
  const sustained = sweep.filter(rate => rate.sustained === true && errorRate(rate) < ERROR_RATE_CEILING)
  if (sustained.length > 0) {
    const chosen = sustained.reduce((best, rate) => (targetRate(rate) > targetRate(best) ? rate : best))
    return { ...chosen, under_slo: false }
  }
  const chosen = sweep.reduce((best, rate) => (targetRate(rate) < targetRate(best) ? rate : best))
  return { ...chosen, under_slo: false }
}
