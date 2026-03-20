import { Bench } from 'tinybench'
import type { BenchmarkResult, ComparisonResult } from './reporter'
import { getGlobalRng, resetGlobalRng } from './rng'
import { detectOutliers, speedupConfidenceInterval, welchTTest } from './stats'

type RunOrder = 'random' | 'sirannon-first' | 'postgres-first'

export interface BenchmarkConfig {
  category: string
  warmupTime?: number
  measureTime?: number
}

export interface TaskDef {
  name: string
  fn: () => void | Promise<void>
  opts?: { async?: boolean }
  beforeAll?: () => void | Promise<void>
  afterAll?: () => void | Promise<void>
  beforeEach?: () => void | Promise<void>
  afterEach?: () => void | Promise<void>
}

export interface ComparisonPair {
  workload: string
  dataSize: number
  framing: string
  sirannon: TaskDef
  postgres: TaskDef
  beforeRun?: () => void | Promise<void>
}

const MS_TO_NS = 1_000_000

function extractResult(task: Awaited<ReturnType<Bench['run']>>[number]): BenchmarkResult {
  const result = task.result
  if (!result || result.state !== 'completed') {
    throw new Error(`Task '${task.name}' did not complete successfully (state: ${result?.state ?? 'unknown'})`)
  }

  const lat = result.latency
  const thr = result.throughput

  const meanNs = lat.mean * MS_TO_NS
  const sdNs = lat.sd * MS_TO_NS

  return {
    name: task.name,
    opsPerSec: thr.mean,
    meanNs,
    p50Ns: lat.p50 * MS_TO_NS,
    p75Ns: lat.p75 * MS_TO_NS,
    p99Ns: lat.p99 * MS_TO_NS,
    p999Ns: lat.p999 * MS_TO_NS,
    minNs: lat.min * MS_TO_NS,
    maxNs: lat.max * MS_TO_NS,
    sdNs,
    cv: lat.mean > 0 ? lat.sd / lat.mean : 0,
    moe: lat.moe * MS_TO_NS,
    samples: lat.samplesCount,
  }
}

export async function runBenchmark(config: BenchmarkConfig, tasks: TaskDef[]): Promise<BenchmarkResult[]> {
  const bench = new Bench({
    warmupTime: config.warmupTime ?? 5_000,
    time: config.measureTime ?? 10_000,
    throws: true,
  })

  for (const task of tasks) {
    bench.add(task.name, task.fn, {
      async: task.opts?.async,
      beforeEach: task.beforeEach,
      afterEach: task.afterEach,
    })
  }

  const completed = await bench.run()

  for (const task of tasks) {
    await task.afterAll?.()
  }

  return completed.map(extractResult)
}

function resolveRunOrder(runIndex?: number): { sirannonFirst: boolean; label: string } {
  const env = (process.env.BENCH_RUN_ORDER ?? 'random') as RunOrder
  if (env === 'sirannon-first') return { sirannonFirst: true, label: 'sirannon-first (forced)' }
  if (env === 'postgres-first') return { sirannonFirst: false, label: 'postgres-first (forced)' }

  if (runIndex !== undefined) {
    const sirannonFirst = runIndex % 2 === 0
    return { sirannonFirst, label: sirannonFirst ? 'sirannon-first (alternating)' : 'postgres-first (alternating)' }
  }

  const sirannonFirst = getGlobalRng().next() < 0.5
  return { sirannonFirst, label: sirannonFirst ? 'sirannon-first (random)' : 'postgres-first (random)' }
}

async function runEngineSide(
  label: string,
  pairs: ComparisonPair[],
  side: 'sirannon' | 'postgres',
  config: BenchmarkConfig,
  skipCleanup = false,
): Promise<Awaited<ReturnType<Bench['run']>>> {
  const bench = new Bench({
    warmupTime: config.warmupTime ?? 5_000,
    time: config.measureTime ?? 10_000,
    throws: true,
  })

  for (const pair of pairs) {
    const task = pair[side]
    const defaultAsync = side === 'postgres'
    bench.add(`${pair.workload} [${pair.dataSize}]`, task.fn, {
      async: task.opts?.async ?? defaultAsync,
      beforeEach: task.beforeEach,
      afterEach: task.afterEach,
    })
  }

  console.log(`Running ${label} benchmarks (${pairs.length} tasks)...`)
  const tasks = await bench.run()

  if (!skipCleanup) {
    for (const pair of pairs) {
      await pair[side].afterAll?.()
    }
  }

  return tasks
}

async function singleComparison(
  config: BenchmarkConfig,
  pairs: ComparisonPair[],
  skipCleanup = false,
  runIndex?: number,
): Promise<ComparisonResult[]> {
  const { sirannonFirst, label } = resolveRunOrder(runIndex)
  console.log(`Run order: ${label}`)

  const firstSide = sirannonFirst ? 'sirannon' : 'postgres'
  const secondSide = sirannonFirst ? 'postgres' : 'sirannon'

  const firstTasks = await runEngineSide(firstSide, pairs, firstSide, config, skipCleanup)

  global.gc?.()
  await sleep(1_000)

  const secondTasks = await runEngineSide(secondSide, pairs, secondSide, config, skipCleanup)

  const sirannonTasks = sirannonFirst ? firstTasks : secondTasks
  const postgresTasks = sirannonFirst ? secondTasks : firstTasks

  const results: ComparisonResult[] = []
  for (let i = 0; i < pairs.length; i++) {
    const pair = pairs[i]
    const sirannonResult = extractResult(sirannonTasks[i])
    const postgresResult = extractResult(postgresTasks[i])

    results.push({
      workload: pair.workload,
      dataSize: pair.dataSize,
      sirannon: sirannonResult,
      postgres: postgresResult,
      speedup: postgresResult.opsPerSec > 0 ? sirannonResult.opsPerSec / postgresResult.opsPerSec : Infinity,
      framing: pair.framing,
    })
  }

  return results
}

export async function runComparison(config: BenchmarkConfig, pairs: ComparisonPair[]): Promise<ComparisonResult[]> {
  const totalRuns = Number(process.env.BENCH_RUNS ?? 5)

  if (totalRuns <= 1) {
    return singleComparison(config, pairs)
  }

  const perPairSirannonOps: number[][] = pairs.map(() => [])
  const perPairPostgresOps: number[][] = pairs.map(() => [])

  let lastResults: ComparisonResult[] = []

  for (let run = 0; run < totalRuns; run++) {
    console.log(`\n--- Run ${run + 1}/${totalRuns} ---`)
    resetGlobalRng()

    for (const pair of pairs) {
      await pair.beforeRun?.()
    }

    const isLast = run === totalRuns - 1
    const results = await singleComparison(config, pairs, !isLast, run)

    for (let i = 0; i < results.length; i++) {
      perPairSirannonOps[i].push(results[i].sirannon.opsPerSec)
      perPairPostgresOps[i].push(results[i].postgres.opsPerSec)
    }

    lastResults = results

    if (!isLast) {
      global.gc?.()
      await sleep(2_000)
    }
  }

  for (let i = 0; i < lastResults.length; i++) {
    const sOps = perPairSirannonOps[i]
    const pOps = perPairPostgresOps[i]

    const avgSirannon = sOps.reduce((a, b) => a + b, 0) / sOps.length
    const avgPostgres = pOps.reduce((a, b) => a + b, 0) / pOps.length

    lastResults[i].sirannon.opsPerSec = avgSirannon
    lastResults[i].postgres.opsPerSec = avgPostgres
    lastResults[i].speedup = avgPostgres > 0 ? avgSirannon / avgPostgres : Infinity
    lastResults[i].sirannonSamples = sOps
    lastResults[i].postgresSamples = pOps
    lastResults[i].runs = totalRuns

    if (totalRuns >= 5) {
      lastResults[i].significance = welchTTest(sOps, pOps)
      lastResults[i].speedupCI = speedupConfidenceInterval(sOps, pOps, 0.95, BigInt(process.env.BENCH_SEED ?? '42'))
      const sOutliers = detectOutliers(sOps)
      const pOutliers = detectOutliers(pOps)
      lastResults[i].outliers = {
        sirannon: { count: sOutliers.count, percentage: sOutliers.percentage },
        postgres: { count: pOutliers.count, percentage: pOutliers.percentage },
      }
    }
  }

  if (totalRuns >= 2 && totalRuns < 5) {
    console.log(`\nNote: ${totalRuns} runs collected. Statistical analysis requires >= 5 runs.`)
  }

  return lastResults
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
