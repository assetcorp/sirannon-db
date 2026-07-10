// Run one engine through every workload and aggregate the numbers.
//
// For each workload the harness prepares a known table state, then sweeps a set of target request
// rates. At each rate it runs several independent measured passes and takes the median throughput
// with a bootstrap confidence interval, so one noisy pass cannot set the headline. Latency is the
// median of each pass's corrected percentiles. From the sweep it picks an operating point: the
// highest offered rate the engine sustained while holding p99 under the disclosed service-level
// target. Before the sweep it measures the load client's own throughput ceiling, so a rate that
// falls short can be charged to the server or the client on evidence, never guessed.

import type { Config } from './config.ts'
import type { Driver } from './drivers/driver.ts'
import { type ClientCeiling, type LoadResult, type RunOp, measureClientCeiling, runOpenLoop } from './loadgen.ts'
import { SeededRng, ZipfianGenerator } from './rng.ts'
import { median, summarizeMetric } from './stats.ts'
import { type OperationContext, type Workload, buildWorkloads, pickOperation } from './workloads.ts'

const ERROR_RATE_CEILING = 0.01
const CEILING_SAFETY = 0.9
const CEILING_MIN_SECONDS = 2
const CEILING_MAX_SECONDS = 4

const MIN_SEED_ROWS_PER_SEC = 20_000
const WORKLOAD_DEADLINE_SAFETY = 3
const WORKLOAD_DEADLINE_FLOOR_MS = 120_000

function workloadDeadlineMs(config: Config, ceilingSeconds: number): number {
  if (config.workloadTimeoutMs > 0) {
    return config.workloadTimeoutMs
  }
  const seedSeconds = config.dataSize / MIN_SEED_ROWS_PER_SEC
  const sweepSeconds =
    ceilingSeconds + config.targetRates.length * config.runs * (config.warmupSeconds + config.measureSeconds)
  return Math.max(WORKLOAD_DEADLINE_FLOOR_MS, Math.ceil((seedSeconds + sweepSeconds) * WORKLOAD_DEADLINE_SAFETY) * 1000)
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
}

export interface WorkloadResult {
  workload: string
  category: string
  runs: number
  operating_point: Record<string, unknown>
  sweep: Record<string, unknown>[]
}

function makeRunOp(driver: Driver, workload: Workload, rng: SeededRng, zipf: ZipfianGenerator, dataSize: number): RunOp {
  const operations = workload.operations
  const useSqlite = driver.dialect === 'sqlite'

  return async (): Promise<boolean> => {
    const operation = pickOperation(rng, operations)
    const ctx: OperationContext = { rng, zipf, dataSize }
    const params = operation.params(ctx)
    try {
      if (operation.kind === 'read') {
        await driver.read(useSqlite ? operation.sqliteSql : operation.postgresSql, params)
      } else if (operation.kind === 'write') {
        await driver.write(useSqlite ? operation.sqliteSql : operation.postgresSql, params)
      } else {
        const readSql = useSqlite ? operation.sqliteSql : operation.postgresSql
        const writeSql = useSqlite ? operation.writeSqliteSql : operation.writePostgresSql
        const key = params[0]
        const value = params[1]
        await driver.read(readSql, [key])
        await driver.write(writeSql ?? readSql, [value, key])
      }
      return true
    } catch {
      return false
    }
  }
}

async function prepare(driver: Driver, workload: Workload, config: Config): Promise<void> {
  await driver.dropTables([...workload.tables])
  const schema = driver.dialect === 'sqlite' ? workload.sqliteSchema : workload.postgresSchema
  const statements = schema
    .split(';')
    .map(part => part.trim())
    .filter(part => part.length > 0)
  await driver.executeDdl(statements)
  const seedRng = new SeededRng(config.seed)
  const seedTables = workload.seed(seedRng, config.dataSize)
  await driver.seed(seedTables)
}

function classifyPass(pass: LoadResult, ceilingOps: number): { serverSaturated: boolean; clientBound: boolean } {
  if (pass.sustained) {
    return { serverSaturated: false, clientBound: false }
  }
  const clientBound = pass.targetRate > ceilingOps * CEILING_SAFETY
  return { serverSaturated: !clientBound, clientBound }
}

async function measureRate(
  makeOp: () => RunOp,
  targetRate: number,
  config: Config,
  ceilingOps: number,
): Promise<Record<string, unknown>> {
  const passes: LoadResult[] = []
  for (let run = 0; run < config.runs; run++) {
    const runOp = makeOp()
    passes.push(
      await runOpenLoop(runOp, targetRate, config.warmupSeconds, config.measureSeconds, config.maxInFlight),
    )
  }

  const throughputSamples = passes.map(pass => pass.achievedRate)
  const summary = summarizeMetric(throughputSamples, 0.95, config.seed)
  const classifications = passes.map(pass => classifyPass(pass, ceilingOps))
  const sustainedVotes = passes.filter(pass => pass.sustained).length
  const saturatedVotes = classifications.filter(entry => entry.serverSaturated).length
  const clientVotes = classifications.filter(entry => entry.clientBound).length
  const majority = config.runs / 2.0

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
    sustained: sustainedVotes > majority,
    server_saturated: saturatedVotes > majority,
    client_bound: clientVotes > majority,
    error_rate: median(passes.map(pass => pass.errorRate)),
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

async function runWorkload(
  driver: Driver,
  workload: Workload,
  config: Config,
  ceilingOps: number,
): Promise<WorkloadResult> {
  await prepare(driver, workload, config)
  const zipf = new ZipfianGenerator(config.dataSize)
  const makeOp = (): RunOp => makeRunOp(driver, workload, new SeededRng(config.seed), zipf, config.dataSize)

  const sweep: Record<string, unknown>[] = []
  for (const rate of config.targetRates) {
    sweep.push(await measureRate(makeOp, rate, config, ceilingOps))
  }
  const operatingPoint = selectOperatingPoint(sweep, config.sloP99Ms)
  return {
    workload: workload.name,
    category: workload.category,
    runs: config.runs,
    operating_point: operatingPoint,
    sweep,
  }
}

export async function runEngine(driver: Driver, config: Config): Promise<EngineResult> {
  const catalogue = buildWorkloads()
  const ceilingSeconds = Math.min(Math.max(config.measureSeconds, CEILING_MIN_SECONDS), CEILING_MAX_SECONDS)
  const trivialOp: RunOp = async () => {
    try {
      await driver.read('SELECT 1', [])
      return true
    } catch {
      return false
    }
  }
  const deadlineMs = workloadDeadlineMs(config, ceilingSeconds)
  const clientCeiling = await withDeadline(
    measureClientCeiling(trivialOp, ceilingSeconds, config.maxInFlight),
    deadlineMs,
    'client-ceiling measurement',
  )

  const results: WorkloadResult[] = []
  let clientBoundAny = false
  for (const name of config.workloads) {
    const workload = catalogue.get(name)
    if (workload === undefined) {
      throw new Error(`unknown workload ${JSON.stringify(name)}; known workloads are ${[...catalogue.keys()].sort().join(', ')}`)
    }
    const result = await withDeadline(
      runWorkload(driver, workload, config, clientCeiling.ceilingOps),
      deadlineMs,
      `workload ${name} (${driver.name}, ${config.dataSize} rows)`,
    )
    if (result.sweep.some(rate => rate.client_bound === true)) {
      clientBoundAny = true
    }
    results.push(result)
  }
  return { workloads: results, clientCeiling, clientBoundAny }
}
