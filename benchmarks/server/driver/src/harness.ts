import { classifyPass, majorityVerdict } from './classify.ts'
import type { Config } from './config.ts'
import type { Driver } from './drivers/driver.ts'
import { engineCpuBlock, withEngineCores } from './engine-cpu.ts'
import { FailureInterner, summarizeFailures } from './failures.ts'
import { type ClientCeiling, type LoadResult, measureClientCeiling, type RunOp, runOpenLoop } from './loadgen.ts'
import { makeRunOp, operationCost } from './operations.ts'
import { SeededRng, ZipfianGenerator } from './rng.ts'
import { median, summarizeMetric } from './stats.ts'
import { buildWorkloads } from './workloads/catalogue.ts'
import type { Workload } from './workloads/workload.ts'

const ERROR_RATE_CEILING = 0.01
const CEILING_MIN_SECONDS = 2
const CEILING_MAX_SECONDS = 4
const CEILING_STATEMENT = 'SELECT 1'
const SOAK_BUCKET_SECONDS = 30

const MIN_SEED_ROWS_PER_SEC = 20_000
const WORKLOAD_DEADLINE_SAFETY = 3
const WORKLOAD_DEADLINE_FLOOR_MS = 120_000

function progress(message: string): void {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${message}`)
}

function workloadDeadlineMs(config: Config, ceilingSeconds: number): number {
  if (config.workloadTimeoutMs > 0) {
    return config.workloadTimeoutMs
  }
  const seedSeconds = config.dataSize / MIN_SEED_ROWS_PER_SEC
  const sweepSeconds =
    ceilingSeconds + config.targetRates.length * config.runs * (config.warmupSeconds + config.measureSeconds)
  const soakSeconds = config.soakSeconds > 0 ? config.warmupSeconds + config.soakSeconds : 0
  return Math.max(
    WORKLOAD_DEADLINE_FLOOR_MS,
    Math.ceil((seedSeconds + sweepSeconds + soakSeconds) * WORKLOAD_DEADLINE_SAFETY) * 1000,
  )
}

function withDeadline<T>(work: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} exceeded its ${Math.round(ms / 1000)}s deadline and was aborted as a stall`))
    }, ms)
  })
  return Promise.race([work, deadline]).finally(() => {
    if (timer !== undefined) {
      clearTimeout(timer)
    }
  })
}

export interface EngineResult {
  workloads: WorkloadResult[]
  clientCeiling: ClientCeiling
  clientBoundAny: boolean
  indeterminateAny: boolean
}

export interface WorkloadResult {
  workload: string
  category: string
  runs: number
  operation_cost: Record<string, unknown>
  seed: Record<string, unknown>
  operating_point: Record<string, unknown>
  sweep: Record<string, unknown>[]
  sweep_stopped_early: boolean
  soak: Record<string, unknown> | null
}

async function prepare(driver: Driver, workload: Workload, config: Config): Promise<Record<string, unknown>> {
  await driver.dropTables([...workload.tables])
  const schema = driver.dialect === 'sqlite' ? workload.sqliteSchema : workload.postgresSchema
  const statements = schema
    .split(';')
    .map(part => part.trim())
    .filter(part => part.length > 0)
  await driver.executeDdl(statements)
  const seedRng = new SeededRng(config.seed)
  const seedTables = workload.seed(seedRng, config.dataSize)
  const counter = { rows: 0 }
  const counted = seedTables.map(table => ({
    ...table,
    rows: (function* (source: Iterable<unknown[]>) {
      for (const row of source) {
        counter.rows += 1
        yield row
      }
    })(table.rows),
  }))
  const seedStart = performance.now()
  const ticker = setInterval(() => {
    const elapsed = (performance.now() - seedStart) / 1000
    progress(`seed ${workload.name}: ${counter.rows.toLocaleString('en-US')} rows in ${Math.round(elapsed)}s`)
  }, 10_000)
  let seconds: number
  let seedCores: number | null = null
  try {
    seedCores = (await withEngineCores(config.engineCgroup, () => driver.seed(counted))).coresUsed
  } finally {
    clearInterval(ticker)
    seconds = (performance.now() - seedStart) / 1000
  }
  progress(
    `seed ${workload.name}: done, ${counter.rows.toLocaleString('en-US')} rows in ${seconds.toFixed(1)}s` +
      (seconds > 0 ? ` (${Math.round(counter.rows / seconds).toLocaleString('en-US')} rows/s)` : '') +
      (seedCores !== null ? ` (engine ${seedCores.toFixed(1)} cores)` : ''),
  )
  return {
    rows: counter.rows,
    seconds,
    rows_per_second: seconds > 0 ? counter.rows / seconds : 0,
    engine_cpu: engineCpuBlock([seedCores], config.engineCpus),
    note:
      'Seeding is disclosed, never compared: each engine loads through its own fastest documented ' +
      'path, and Sirannon seeds with durability off while PostgreSQL seeds inside one transaction.',
  }
}

function generatorBlock(passes: LoadResult[], config: Config): Record<string, unknown> {
  return {
    max_in_flight: config.maxInFlight,
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

async function measureRate(
  makeOp: () => RunOp,
  targetRate: number,
  config: Config,
  ceilingOps: number,
): Promise<Record<string, unknown>> {
  const passes: LoadResult[] = []
  const coreSamples: Array<number | null> = []
  for (let run = 0; run < config.runs; run++) {
    const runOp = makeOp()
    const measured = await withEngineCores(config.engineCgroup, () =>
      runOpenLoop(runOp, targetRate, config.warmupSeconds, config.measureSeconds, config.maxInFlight),
    )
    passes.push(measured.result)
    coreSamples.push(measured.coresUsed)
  }

  const throughputSamples = passes.map(pass => pass.achievedRate)
  const summary = summarizeMetric(throughputSamples, 0.95, config.seed)
  const classifications = passes.map(pass => classifyPass(pass, ceilingOps))
  const verdict = majorityVerdict(classifications.map(entry => entry.verdict))

  return {
    target_rate: targetRate,
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
    generator: generatorBlock(passes, config),
    engine_cpu: engineCpuBlock(coreSamples, config.engineCpus),
    sustained: verdict === 'sustained',
    server_saturated: verdict === 'server_saturated' || verdict === 'both',
    client_bound: verdict === 'client_bound' || verdict === 'both',
    limit_verdict: verdict,
    verdict_samples: classifications.map(entry => entry.verdict),
    error_rate: median(passes.map(pass => pass.errorRate)),
    failures: summarizeFailures(passes.map(pass => pass.failures)),
  }
}

function selectOperatingPoint(sweep: Record<string, unknown>[], sloP99Ms: number): Record<string, unknown> {
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

async function runSoak(
  makeOp: () => RunOp,
  operatingPoint: Record<string, unknown>,
  config: Config,
  workloadName: string,
): Promise<Record<string, unknown>> {
  if (operatingPoint.sustained !== true) {
    return {
      skipped: true,
      reason: 'the sweep found no rate this engine sustained, so there is no operating point to hold',
    }
  }
  const targetRate = operatingPoint.target_rate as number
  progress(`soak ${workloadName}: holding ${targetRate.toLocaleString('en-US')}/s for ${config.soakSeconds}s`)
  const soakStart = performance.now()
  const ticker = setInterval(() => {
    const elapsed = Math.round((performance.now() - soakStart) / 1000)
    progress(`soak ${workloadName}: ${elapsed}s of ${Math.round(config.warmupSeconds + config.soakSeconds)}s`)
  }, 60_000)
  let pass: LoadResult
  let soakCores: number | null
  try {
    const measured = await withEngineCores(config.engineCgroup, () =>
      runOpenLoop(
        makeOp(),
        targetRate,
        config.warmupSeconds,
        config.soakSeconds,
        config.maxInFlight,
        SOAK_BUCKET_SECONDS,
      ),
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

async function runWorkload(
  driver: Driver,
  workload: Workload,
  config: Config,
  ceilingOps: number,
): Promise<WorkloadResult> {
  const seed = await prepare(driver, workload, config)
  const zipf = new ZipfianGenerator(config.dataSize)
  const makeOp = (): RunOp => makeRunOp(driver, workload, new SeededRng(config.seed), zipf, config.dataSize)

  const sweep: Record<string, unknown>[] = []
  let stoppedEarly = false
  let stepsPastSaturation = -1
  for (const rate of config.targetRates) {
    progress(`sweep ${workload.name}: measuring ${rate.toLocaleString('en-US')}/s (${config.runs} passes)`)
    const point = await measureRate(makeOp, rate, config, ceilingOps)
    sweep.push(point)
    const p99 = (point.latency_ms as { p99: number }).p99
    progress(
      `sweep ${workload.name}: ${rate.toLocaleString('en-US')}/s ${point.sustained === true ? 'sustained' : 'not sustained'}, p99 ${p99.toFixed(1)}ms`,
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
  const operatingPoint = selectOperatingPoint(sweep, config.sloP99Ms)
  const soak =
    config.soakSeconds > 0 && config.soakWorkloads.includes(workload.name)
      ? await runSoak(makeOp, operatingPoint, config, workload.name)
      : null
  return {
    workload: workload.name,
    category: workload.category,
    runs: config.runs,
    operation_cost: operationCost(driver, workload),
    seed,
    operating_point: operatingPoint,
    sweep,
    sweep_stopped_early: stoppedEarly,
    soak,
  }
}

export async function runEngine(driver: Driver, config: Config): Promise<EngineResult> {
  const catalogue = buildWorkloads()
  for (const name of config.soakWorkloads) {
    if (!catalogue.has(name)) {
      throw new Error(
        `unknown soak workload ${JSON.stringify(name)}; known workloads are ${[...catalogue.keys()].sort().join(', ')}`,
      )
    }
  }
  const ceilingSeconds = Math.min(Math.max(config.measureSeconds, CEILING_MIN_SECONDS), CEILING_MAX_SECONDS)
  const ceilingFailures = new FailureInterner(driver.failureClassifier)
  const trivialOp: RunOp = async () => {
    try {
      await driver.read(CEILING_STATEMENT, [])
      return null
    } catch (err) {
      return ceilingFailures.categorize(err)
    }
  }
  const deadlineMs = workloadDeadlineMs(config, ceilingSeconds)
  const clientCeiling = await withDeadline(
    measureClientCeiling(trivialOp, ceilingSeconds, config.maxInFlight, CEILING_STATEMENT),
    deadlineMs,
    'client-ceiling measurement',
  )

  const results: WorkloadResult[] = []
  let clientBoundAny = false
  let indeterminateAny = false
  for (const name of config.workloads) {
    const workload = catalogue.get(name)
    if (workload === undefined) {
      throw new Error(
        `unknown workload ${JSON.stringify(name)}; known workloads are ${[...catalogue.keys()].sort().join(', ')}`,
      )
    }
    const result = await withDeadline(
      runWorkload(driver, workload, config, clientCeiling.ceilingOps),
      deadlineMs,
      `workload ${name} (${driver.name}, ${config.dataSize} rows)`,
    )
    if (result.sweep.some(rate => rate.client_bound === true)) {
      clientBoundAny = true
    }
    if (result.sweep.some(rate => rate.limit_verdict === 'indeterminate')) {
      indeterminateAny = true
    }
    results.push(result)
  }
  return { workloads: results, clientCeiling, clientBoundAny, indeterminateAny }
}
