import { execSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import http from 'node:http'
import { join } from 'node:path'
import { collectSystemInfo } from './config'
import { escapeCsvField, formatLatency } from './reporter'
import { buildManifest, createRunDirectory, runIdFromIso, writeRunArtifact } from './run-manifest'

const COMPOSE_FILE = join(import.meta.dirname, 'docker', 'docker-compose.yml')
const RESULTS_DIR = join(import.meta.dirname, 'results')

const SIRANNON_ENGINE_URL = process.env.SIRANNON_ENGINE_URL ?? 'http://localhost:9878'
const POSTGRES_ENGINE_URL = process.env.POSTGRES_ENGINE_URL ?? 'http://localhost:9879'

const DATA_SIZES = (process.env.BENCH_DATA_SIZES ?? '1000,10000').split(',').map(Number)
const WARMUP_MS = Number(process.env.BENCH_WARMUP_MS ?? 5000)
const MEASURE_MS = Number(process.env.BENCH_MEASURE_MS ?? 10000)
const WORKLOADS = (
  process.env.BENCH_WORKLOADS ?? 'point-select,bulk-insert,batch-update,ycsb-a,ycsb-b,ycsb-c,ycsb-f,tpc-c-derived'
).split(',')

/**
 * The published engine track is a co-located server-vs-server comparison: the load driver
 * reaches Sirannon through the SirannonClient SDK over HTTP into the real server front-end,
 * and Postgres through the native pg driver, both over loopback on one host. Each side runs
 * on the same total CPU budget split between its load driver and its database server. These
 * values name what the recorded run measured so the generated page can disclose it; they are
 * read from the same environment the Docker compose file sets, so the disclosure never drifts
 * from the containers that produced the numbers.
 */
const DELIVERY_DISCLOSURE = {
  sirannon: 'client-server-http',
  postgres: 'client-server',
  transport: 'loopback',
  cpusPerSide: Number(process.env.BENCH_CPUS ?? 2),
  sirannonSplit: process.env.BENCH_SIRANNON_SPLIT ?? 'driver and server share the container CPU budget',
  postgresSplit: process.env.BENCH_POSTGRES_SPLIT ?? 'driver container plus database container share the CPU budget',
} as const

function run(cmd: string, opts?: { ignoreError?: boolean }) {
  console.log(`> ${cmd}`)
  try {
    execSync(cmd, { stdio: 'inherit' })
  } catch (err) {
    if (!opts?.ignoreError) throw err
  }
}

async function waitForHealth(url: string, label: string, maxRetries = 30) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await fetch(`${url}/health`)
      if (res.ok) {
        console.log(`${label} is healthy`)
        return
      }
    } catch {}
    await new Promise(r => setTimeout(r, 2000))
  }
  throw new Error(`${label} did not become healthy after ${maxRetries * 2}s`)
}

async function postJson(url: string, body: unknown, timeoutMs = 30_000): Promise<unknown> {
  const jsonBody = JSON.stringify(body)
  const parsed = new URL(url)

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(jsonBody),
        },
      },
      res => {
        const chunks: Buffer[] = []
        res.on('data', (chunk: Buffer) => chunks.push(chunk))
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf-8')
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(JSON.parse(text))
          } else {
            reject(new Error(`POST ${url} failed: ${res.statusCode} ${text}`))
          }
        })
      },
    )

    const timer = setTimeout(() => {
      req.destroy(new Error(`POST ${url} timed out after ${timeoutMs}ms`))
    }, timeoutMs)

    req.on('error', err => {
      clearTimeout(timer)
      reject(err)
    })
    req.on('close', () => clearTimeout(timer))
    req.write(jsonBody)
    req.end()
  })
}

async function getJson(url: string, timeoutMs = 15_000): Promise<unknown> {
  const parsed = new URL(url)
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: parsed.hostname, port: parsed.port, path: parsed.pathname, method: 'GET' },
      res => {
        const chunks: Buffer[] = []
        res.on('data', (chunk: Buffer) => chunks.push(chunk))
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf-8')
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(JSON.parse(text))
          } else {
            reject(new Error(`GET ${url} failed: ${res.statusCode} ${text}`))
          }
        })
      },
    )
    const timer = setTimeout(() => req.destroy(new Error(`GET ${url} timed out after ${timeoutMs}ms`)), timeoutMs)
    req.on('error', err => {
      clearTimeout(timer)
      reject(err)
    })
    req.on('close', () => clearTimeout(timer))
    req.end()
  })
}

function estimateBenchmarkTimeoutMs(
  workloads: string[],
  dataSizes: number[],
  warmupMs: number,
  measureMs: number,
): number {
  const numRuns = workloads.length * dataSizes.length
  const perRunOverhead = 15_000
  const estimated = numRuns * (warmupMs + measureMs + perRunOverhead)
  return Math.max(estimated * 2, 120_000)
}

interface EngineResult {
  engine: string
  workload: string
  dataSize: number
  results: Array<{
    name: string
    opsPerSec: number
    meanNs: number
    p50Ns: number
    p75Ns: number
    p99Ns: number
    p999Ns: number
    samples: number
    cv: number
  }>
}

function pad(str: string, width: number, align: 'left' | 'right' = 'right'): string {
  return align === 'left' ? str.padEnd(width) : str.padStart(width)
}

function fmtOps(ops: number): string {
  if (ops >= 1_000_000) return `${(ops / 1_000_000).toFixed(2)}M`
  if (ops >= 1_000) return `${(ops / 1_000).toFixed(2)}K`
  return ops.toFixed(2)
}

type DeliveryMode = 'client-server-http' | 'client-server' | 'embedded'

interface ConcurrentDataPoint {
  concurrency: number
  readRatio: number
  model: string
  delivery: DeliveryMode
  totalOps: number
  opsPerSec: number
  p50Ns: number
  p99Ns: number
}

function workloadLabel(readRatio: number): string {
  if (readRatio === 1.0) return 'read-only'
  if (readRatio === 0.5) return 'mixed-50/50'
  return `read-${Math.round(readRatio * 100)}%`
}

function printScalingTable(
  title: string,
  model: string,
  sirannonPoints: ConcurrentDataPoint[],
  postgresPoints: ConcurrentDataPoint[],
) {
  const filtered = sirannonPoints.filter(p => p.model === model)
  if (filtered.length === 0) return

  console.log(`\n${'='.repeat(120)}`)
  console.log(title)
  console.log('='.repeat(120))

  const header = [
    pad('Workload', 14, 'left'),
    pad('N', 4),
    pad('Sirannon ops/s', 16),
    pad('Postgres ops/s', 16),
    pad('Speedup', 10),
    pad('S P50', 12),
    pad('S P99', 12),
    pad('P P50', 12),
    pad('P P99', 12),
  ].join(' | ')

  console.log('-'.repeat(header.length))
  console.log(header)
  console.log('-'.repeat(header.length))

  for (const sp of filtered) {
    const pp = postgresPoints.find(
      p => p.model === model && p.concurrency === sp.concurrency && p.readRatio === sp.readRatio,
    )
    if (!pp) continue

    const speedup = pp.opsPerSec > 0 ? sp.opsPerSec / pp.opsPerSec : Infinity
    console.log(
      [
        pad(workloadLabel(sp.readRatio), 14, 'left'),
        pad(String(sp.concurrency), 4),
        pad(fmtOps(sp.opsPerSec), 16),
        pad(fmtOps(pp.opsPerSec), 16),
        pad(`${speedup.toFixed(2)}x`, 10),
        pad(formatLatency(sp.p50Ns), 12),
        pad(formatLatency(sp.p99Ns), 12),
        pad(formatLatency(pp.p50Ns), 12),
        pad(formatLatency(pp.p99Ns), 12),
      ].join(' | '),
    )
  }

  console.log('-'.repeat(header.length))
}

function printComparisonTable(sirannonResults: EngineResult[], postgresResults: EngineResult[]) {
  const header = [
    pad('Workload', 25, 'left'),
    pad('N Rows', 8),
    pad('Sirannon ops/s', 16),
    pad('Postgres ops/s', 16),
    pad('Speedup', 10),
    pad('S P50', 12),
    pad('S P99', 12),
    pad('S CV', 8),
  ].join(' | ')

  const separator = '-'.repeat(header.length)
  console.log(`\n${separator}`)
  console.log(header)
  console.log(separator)

  for (const sResult of sirannonResults) {
    const pResult = postgresResults.find(r => r.workload === sResult.workload && r.dataSize === sResult.dataSize)
    if (!pResult) continue

    for (let i = 0; i < sResult.results.length; i++) {
      const sr = sResult.results[i]
      const pr = pResult.results[i]
      if (!pr) continue

      const speedup = pr.opsPerSec > 0 ? sr.opsPerSec / pr.opsPerSec : Infinity
      const cvPct = sr.cv * 100
      const cvStr = `${cvPct.toFixed(1)}%${cvPct > 15 ? ' [!]' : ''}`

      console.log(
        [
          pad(sr.name, 25, 'left'),
          pad(sResult.dataSize.toLocaleString(), 8),
          pad(fmtOps(sr.opsPerSec), 16),
          pad(fmtOps(pr.opsPerSec), 16),
          pad(`${speedup.toFixed(2)}x`, 10),
          pad(formatLatency(sr.p50Ns), 12),
          pad(formatLatency(sr.p99Ns), 12),
          pad(cvStr, 8),
        ].join(' | '),
      )
    }
  }

  console.log(separator)
}

async function main() {
  mkdirSync(RESULTS_DIR, { recursive: true })
  const systemInfo = collectSystemInfo()

  console.log('Building and starting engine containers...')
  run(`docker compose -f ${COMPOSE_FILE} --profile engine build`)
  run(`docker compose -f ${COMPOSE_FILE} --profile engine up -d`)

  try {
    await waitForHealth(SIRANNON_ENGINE_URL, 'Sirannon engine')
    await waitForHealth(POSTGRES_ENGINE_URL, 'Postgres engine')

    const benchTimeout = estimateBenchmarkTimeoutMs(WORKLOADS, DATA_SIZES, WARMUP_MS, MEASURE_MS)
    console.log(`\nBenchmark timeout: ${Math.round(benchTimeout / 1000)}s per engine`)

    console.log('\nRunning Sirannon engine benchmarks...')
    const sirannonResult = (await postJson(
      `${SIRANNON_ENGINE_URL}/benchmark/all`,
      {
        dataSizes: DATA_SIZES,
        warmupMs: WARMUP_MS,
        measureMs: MEASURE_MS,
        workloads: WORKLOADS,
      },
      benchTimeout,
    )) as { engine: string; results: EngineResult[] }

    console.log('\nRunning Postgres engine benchmarks...')
    const postgresResult = (await postJson(
      `${POSTGRES_ENGINE_URL}/benchmark/all`,
      {
        dataSizes: DATA_SIZES,
        warmupMs: WARMUP_MS,
        measureMs: MEASURE_MS,
        workloads: WORKLOADS,
      },
      benchTimeout,
    )) as { engine: string; results: EngineResult[] }

    printComparisonTable(sirannonResult.results as EngineResult[], postgresResult.results as EngineResult[])

    const sirannonInfo = (await getJson(`${SIRANNON_ENGINE_URL}/info`).catch(() => ({}))) as Record<string, string>
    const postgresInfo = (await getJson(`${POSTGRES_ENGINE_URL}/info`).catch(() => ({}))) as Record<string, string>
    systemInfo.sqliteVersion = sirannonInfo.version ?? ''
    systemInfo.postgresVersion = postgresInfo.version ?? ''

    const createdAtIso = new Date().toISOString()
    const timestamp = createdAtIso.replace(/[:.]/g, '-')
    const runId = runIdFromIso(createdAtIso)
    const runDir = createRunDirectory(
      RESULTS_DIR,
      buildManifest({ runId, createdAt: createdAtIso, category: 'engine', system: systemInfo }),
    )

    const enginePayload = {
      category: 'engine' as const,
      timestamp: createdAtIso,
      system: systemInfo,
      config: {
        dataSizes: DATA_SIZES,
        warmupMs: WARMUP_MS,
        measureMs: MEASURE_MS,
        workloads: WORKLOADS,
        delivery: DELIVERY_DISCLOSURE,
      },
      sirannon: sirannonResult,
      postgres: postgresResult,
    }

    const resultsPath = join(RESULTS_DIR, `engine-${timestamp}.json`)
    writeFileSync(resultsPath, `${JSON.stringify(enginePayload, null, 2)}\n`)
    writeRunArtifact(runDir, 'engine.json', enginePayload)

    console.log(`\nEngine benchmark results written to ${resultsPath}`)
    console.log(`Self-describing run recorded under ${join(runDir)}`)

    const engineCsvHeader = [
      'workload',
      'dataSize',
      'operation',
      'sirannonOpsPerSec',
      'postgresOpsPerSec',
      'speedup',
      'sirannonP50Ns',
      'sirannonP99Ns',
      'postgresP50Ns',
      'postgresP99Ns',
      'sirannonCV',
      'postgresCV',
    ].join(',')
    const engineCsvRows: string[] = []

    for (const sResult of sirannonResult.results as EngineResult[]) {
      const pResult = (postgresResult.results as EngineResult[]).find(
        r => r.workload === sResult.workload && r.dataSize === sResult.dataSize,
      )
      if (!pResult) continue

      for (let i = 0; i < sResult.results.length; i++) {
        const sr = sResult.results[i]
        const pr = pResult.results[i]
        if (!pr) continue
        const speedup = pr.opsPerSec > 0 ? sr.opsPerSec / pr.opsPerSec : Infinity
        engineCsvRows.push(
          [
            escapeCsvField(sResult.workload),
            sResult.dataSize,
            escapeCsvField(sr.name),
            sr.opsPerSec.toFixed(2),
            pr.opsPerSec.toFixed(2),
            speedup.toFixed(4),
            sr.p50Ns.toFixed(0),
            sr.p99Ns.toFixed(0),
            pr.p50Ns.toFixed(0),
            pr.p99Ns.toFixed(0),
            sr.cv.toFixed(4),
            pr.cv.toFixed(4),
          ].join(','),
        )
      }
    }

    const engineCsvPath = join(RESULTS_DIR, `engine-${timestamp}.csv`)
    writeFileSync(engineCsvPath, `${[engineCsvHeader, ...engineCsvRows].join('\n')}\n`)
    console.log(`Engine CSV written to ${engineCsvPath}`)

    const concurrencyLevels = (process.env.BENCH_CONCURRENCY_LEVELS ?? '1,2,4,8,16,32,64').split(',').map(Number)
    const scalingDurationMs = Number(process.env.BENCH_SCALING_DURATION_MS ?? 10_000)
    const scalingTimeout = concurrencyLevels.length * 2 * 2 * (scalingDurationMs + 30_000) * 2

    console.log('\nRunning Sirannon concurrency scaling benchmarks...')
    const sirannonConcResult = (await postJson(
      `${SIRANNON_ENGINE_URL}/benchmark/concurrent`,
      { concurrencyLevels, durationMs: scalingDurationMs, dataSize: 10_000, readRatios: [1.0, 0.5] },
      scalingTimeout,
    )) as { engine: string; results: ConcurrentDataPoint[] }

    console.log('\nRunning Postgres concurrency scaling benchmarks...')
    const postgresConcResult = (await postJson(
      `${POSTGRES_ENGINE_URL}/benchmark/concurrent`,
      { concurrencyLevels, durationMs: scalingDurationMs, dataSize: 10_000, readRatios: [1.0, 0.5] },
      scalingTimeout,
    )) as { engine: string; results: ConcurrentDataPoint[] }

    printScalingTable(
      'Concurrency Scaling: Single Event Loop',
      'event-loop',
      sirannonConcResult.results,
      postgresConcResult.results,
    )
    printScalingTable(
      'Concurrency Scaling: Worker Thread Pool',
      'worker-threads',
      sirannonConcResult.results,
      postgresConcResult.results,
    )

    const scalingPayload = {
      category: 'engine-scaling' as const,
      timestamp: createdAtIso,
      system: systemInfo,
      config: {
        concurrencyLevels,
        durationMs: scalingDurationMs,
        dataSize: 10_000,
        readRatios: [1.0, 0.5],
        delivery: DELIVERY_DISCLOSURE,
      },
      sirannon: sirannonConcResult,
      postgres: postgresConcResult,
    }

    const scalingResultsPath = join(RESULTS_DIR, `engine-scaling-${timestamp}.json`)
    writeFileSync(scalingResultsPath, `${JSON.stringify(scalingPayload, null, 2)}\n`)
    writeRunArtifact(runDir, 'engine-scaling.json', scalingPayload)
    console.log(`\nScaling benchmark results written to ${scalingResultsPath}`)

    const scalingCsvHeader = [
      'model',
      'workload',
      'concurrency',
      'sirannonOpsPerSec',
      'postgresOpsPerSec',
      'speedup',
      'sirannonP50Ns',
      'sirannonP99Ns',
      'postgresP50Ns',
      'postgresP99Ns',
    ].join(',')
    const scalingCsvRows: string[] = []

    const sirannonConcPoints = sirannonConcResult.results
    const postgresConcPoints = postgresConcResult.results

    for (const sp of sirannonConcPoints) {
      const pp = postgresConcPoints.find(
        p => p.model === sp.model && p.concurrency === sp.concurrency && p.readRatio === sp.readRatio,
      )
      if (!pp) continue
      const speedup = pp.opsPerSec > 0 ? sp.opsPerSec / pp.opsPerSec : Infinity
      scalingCsvRows.push(
        [
          escapeCsvField(sp.model),
          escapeCsvField(workloadLabel(sp.readRatio)),
          sp.concurrency,
          sp.opsPerSec.toFixed(2),
          pp.opsPerSec.toFixed(2),
          speedup.toFixed(4),
          sp.p50Ns.toFixed(0),
          sp.p99Ns.toFixed(0),
          pp.p50Ns.toFixed(0),
          pp.p99Ns.toFixed(0),
        ].join(','),
      )
    }

    const scalingCsvPath = join(RESULTS_DIR, `engine-scaling-${timestamp}.csv`)
    writeFileSync(scalingCsvPath, `${[scalingCsvHeader, ...scalingCsvRows].join('\n')}\n`)
    console.log(`Scaling CSV written to ${scalingCsvPath}`)

    await postJson(`${SIRANNON_ENGINE_URL}/cleanup`, {})
    await postJson(`${POSTGRES_ENGINE_URL}/cleanup`, {})
  } finally {
    console.log('\nStopping engine containers...')
    run(`docker compose -f ${COMPOSE_FILE} --profile engine down`, { ignoreError: true })
  }
}

main().catch(err => {
  console.error(err)
  run(`docker compose -f ${COMPOSE_FILE} --profile engine down`, { ignoreError: true })
  process.exit(1)
})
