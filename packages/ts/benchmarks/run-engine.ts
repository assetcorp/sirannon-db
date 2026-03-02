import { execSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { collectSystemInfo } from './config'
import { formatLatency } from './reporter'

const COMPOSE_FILE = join(import.meta.dirname, 'docker', 'docker-compose.yml')
const RESULTS_DIR = join(import.meta.dirname, 'results')

const SIRANNON_ENGINE_URL = process.env.SIRANNON_ENGINE_URL ?? 'http://localhost:9878'
const POSTGRES_ENGINE_URL = process.env.POSTGRES_ENGINE_URL ?? 'http://localhost:9879'

const DATA_SIZES = (process.env.BENCH_DATA_SIZES ?? '1000,10000').split(',').map(Number)
const WARMUP_MS = Number(process.env.BENCH_WARMUP_MS ?? 5000)
const MEASURE_MS = Number(process.env.BENCH_MEASURE_MS ?? 10000)
const WORKLOADS = (
  process.env.BENCH_WORKLOADS ?? 'point-select,range-select,bulk-insert,batch-update,ycsb-a,ycsb-b,ycsb-c,tpc-c-lite'
).split(',')

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

async function postJson(url: string, body: unknown): Promise<unknown> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`POST ${url} failed: ${res.status} ${text}`)
  }
  return res.json()
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

    console.log('\nRunning Sirannon engine benchmarks...')
    const sirannonResult = (await postJson(`${SIRANNON_ENGINE_URL}/benchmark/all`, {
      dataSizes: DATA_SIZES,
      warmupMs: WARMUP_MS,
      measureMs: MEASURE_MS,
      workloads: WORKLOADS,
    })) as { engine: string; results: EngineResult[] }

    console.log('\nRunning Postgres engine benchmarks...')
    const postgresResult = (await postJson(`${POSTGRES_ENGINE_URL}/benchmark/all`, {
      dataSizes: DATA_SIZES,
      warmupMs: WARMUP_MS,
      measureMs: MEASURE_MS,
      workloads: WORKLOADS,
    })) as { engine: string; results: EngineResult[] }

    printComparisonTable(sirannonResult.results as EngineResult[], postgresResult.results as EngineResult[])

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const resultsPath = join(RESULTS_DIR, `engine-${timestamp}.json`)
    writeFileSync(
      resultsPath,
      `${JSON.stringify(
        {
          category: 'engine',
          timestamp: new Date().toISOString(),
          system: systemInfo,
          config: { dataSizes: DATA_SIZES, warmupMs: WARMUP_MS, measureMs: MEASURE_MS, workloads: WORKLOADS },
          sirannon: sirannonResult,
          postgres: postgresResult,
        },
        null,
        2,
      )}\n`,
    )

    console.log(`\nEngine benchmark results written to ${resultsPath}`)

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
