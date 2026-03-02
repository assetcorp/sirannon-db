import { Bench } from 'tinybench'
import type { BenchmarkResult, ComparisonResult } from './reporter'

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

export async function runComparison(config: BenchmarkConfig, pairs: ComparisonPair[]): Promise<ComparisonResult[]> {
  const sirannonBench = new Bench({
    warmupTime: config.warmupTime ?? 5_000,
    time: config.measureTime ?? 10_000,
    throws: true,
  })

  for (const pair of pairs) {
    const task = pair.sirannon
    sirannonBench.add(`${pair.workload} [${pair.dataSize}]`, task.fn, {
      async: task.opts?.async ?? false,
      beforeEach: task.beforeEach,
      afterEach: task.afterEach,
    })
  }

  console.log(`Running Sirannon benchmarks (${pairs.length} tasks)...`)
  const sirannonTasks = await sirannonBench.run()

  for (const pair of pairs) {
    await pair.sirannon.afterAll?.()
  }

  global.gc?.()
  await sleep(1_000)

  const postgresBench = new Bench({
    warmupTime: config.warmupTime ?? 5_000,
    time: config.measureTime ?? 10_000,
    throws: true,
  })

  for (const pair of pairs) {
    const task = pair.postgres
    postgresBench.add(`${pair.workload} [${pair.dataSize}]`, task.fn, {
      async: task.opts?.async ?? true,
      beforeEach: task.beforeEach,
      afterEach: task.afterEach,
    })
  }

  console.log(`Running Postgres benchmarks (${pairs.length} tasks)...`)
  const postgresTasks = await postgresBench.run()

  for (const pair of pairs) {
    await pair.postgres.afterAll?.()
  }

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

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
