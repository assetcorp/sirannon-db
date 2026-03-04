import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'
import pg from 'pg'
import { Database } from '../../src/core/database'
import { collectSystemInfo, loadConfig } from '../config'
import { isPostgresAvailable } from '../postgres-engine'
import type { BenchmarkResult, ComparisonResult } from '../reporter'
import { writeResults } from '../reporter'
import { resetGlobalRng, SeededRng } from '../rng'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'

const FRAMING =
  'Concurrency scaling: measures aggregate throughput as independent clients increase ' +
  'from 1 to 64. Tests two deployment models: (1) single event loop, where Sirannon ' +
  'queries serialize and Postgres queries overlap via async pool; (2) worker thread pool, ' +
  'where each engine gets N threads with their own connections. Two workloads: read-only ' +
  '(WAL concurrent readers) and 50/50 mixed (single-writer contention). Sirannon workers ' +
  'each open their own Database instance to the same WAL-mode file. Postgres workers ' +
  'share a pg.Pool with max=N connections, matching real production deployment patterns.'

const CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32, 64]
const READ_RATIOS = [1.0, 0.5]
const DATA_SIZE = 10_000
const DURATION_MS = Number(process.env.BENCH_SCALING_DURATION_MS ?? 10_000)
const WORKER_PATH = join(import.meta.dirname, 'worker-bootstrap.mjs')
const BASE_SEED = 42

interface WorkerResult {
  ops: number
  latencySamplesNs: number[]
}

interface ScalingDataPoint {
  concurrency: number
  readRatio: number
  model: 'event-loop' | 'worker-threads'
  sirannonOps: number
  postgresOps: number
  sirannonP50Ns: number
  sirannonP99Ns: number
  postgresP50Ns: number
  postgresP99Ns: number
  speedup: number
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const idx = Math.ceil(p * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

function aggregateLatencies(allSamples: number[][]): { p50Ns: number; p99Ns: number } {
  const merged = allSamples.flat().sort((a, b) => a - b)
  return { p50Ns: percentile(merged, 0.5), p99Ns: percentile(merged, 0.99) }
}

function spawnWorker(config: Record<string, unknown>): Promise<WorkerResult> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_PATH, { workerData: config })
    worker.on('message', (result: WorkerResult) => resolve(result))
    worker.on('error', reject)
    worker.on('exit', code => {
      if (code !== 0) reject(new Error(`Worker exited with code ${code}`))
    })
  })
}

function runEventLoopSirannon(
  dbPath: string,
  concurrency: number,
  readRatio: number,
): { totalOps: number; p50Ns: number; p99Ns: number } {
  const db = new Database('scaling-el', dbPath, { readPoolSize: 1, walMode: true })
  db.execute('PRAGMA busy_timeout = 5000')
  db.execute('PRAGMA synchronous = NORMAL')

  try {
    const rng = new SeededRng(BigInt(BASE_SEED))
    const zipf = new ZipfianGenerator(DATA_SIZE, 0.99, rng)

    let ops = 0
    const latencySamples: number[] = []
    const deadline = Date.now() + DURATION_MS

    while (Date.now() < deadline) {
      for (let i = 0; i < concurrency; i++) {
        const isRead = rng.next() < readRatio
        const id = zipf.next() + 1
        const start = process.hrtime.bigint()
        if (isRead) {
          db.query('SELECT * FROM users WHERE id = ?', [id])
        } else {
          const age = Math.floor(rng.next() * 80) + 18
          db.execute('UPDATE users SET age = ? WHERE id = ?', [age, id])
        }
        const elapsed = Number(process.hrtime.bigint() - start)
        ops++
        if (latencySamples.length < 10_000) latencySamples.push(elapsed)
      }
    }

    latencySamples.sort((a, b) => a - b)
    return {
      totalOps: ops,
      p50Ns: percentile(latencySamples, 0.5),
      p99Ns: percentile(latencySamples, 0.99),
    }
  } finally {
    db.close()
  }
}

async function runEventLoopPostgres(
  pgConfig: pg.PoolConfig,
  concurrency: number,
  readRatio: number,
): Promise<{ totalOps: number; p50Ns: number; p99Ns: number }> {
  const pool = new pg.Pool({ ...pgConfig, max: concurrency })

  try {
    const rng = new SeededRng(BigInt(BASE_SEED))
    const zipf = new ZipfianGenerator(DATA_SIZE, 0.99, rng)

    let ops = 0
    const latencySamples: number[] = []
    const deadline = Date.now() + DURATION_MS

    while (Date.now() < deadline) {
      const batch: Promise<void>[] = []
      for (let i = 0; i < concurrency; i++) {
        const isRead = rng.next() < readRatio
        const id = zipf.next() + 1
        const start = process.hrtime.bigint()

        if (isRead) {
          batch.push(
            pool.query({ text: 'SELECT * FROM users WHERE id = $1', values: [id] }).then(() => {
              const elapsed = Number(process.hrtime.bigint() - start)
              ops++
              if (latencySamples.length < 10_000) latencySamples.push(elapsed)
            }),
          )
        } else {
          const age = Math.floor(rng.next() * 80) + 18
          batch.push(
            pool.query({ text: 'UPDATE users SET age = $1 WHERE id = $2', values: [age, id] }).then(() => {
              const elapsed = Number(process.hrtime.bigint() - start)
              ops++
              if (latencySamples.length < 10_000) latencySamples.push(elapsed)
            }),
          )
        }
      }
      await Promise.all(batch)
    }

    latencySamples.sort((a, b) => a - b)
    return {
      totalOps: ops,
      p50Ns: percentile(latencySamples, 0.5),
      p99Ns: percentile(latencySamples, 0.99),
    }
  } finally {
    await pool.end()
  }
}

async function runWorkerThreads(
  engine: 'sirannon' | 'postgres',
  concurrency: number,
  readRatio: number,
  sirannonDbPath: string,
  pgConfig: pg.PoolConfig,
): Promise<{ totalOps: number; p50Ns: number; p99Ns: number }> {
  const workers: Promise<WorkerResult>[] = []

  for (let i = 0; i < concurrency; i++) {
    const config: Record<string, unknown> = {
      engine,
      durationMs: DURATION_MS,
      dataSize: DATA_SIZE,
      readRatio,
      seed: BASE_SEED + i + 1,
    }
    if (engine === 'sirannon') {
      config.sirannonDbPath = sirannonDbPath
    } else {
      config.pgConfig = pgConfig
    }
    workers.push(spawnWorker(config))
  }

  const results = await Promise.all(workers)
  const totalOps = results.reduce((sum, r) => sum + r.ops, 0)
  const latencies = aggregateLatencies(results.map(r => r.latencySamplesNs))
  return { totalOps, ...latencies }
}

function setupSirannonDb(tempDir: string): string {
  const dbPath = join(tempDir, 'scaling.db')
  const db = new Database('scaling-setup', dbPath, { readPoolSize: 1, walMode: true })
  db.execute('PRAGMA synchronous = NORMAL')

  for (const stmt of microSchemaSqlite
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)) {
    db.execute(stmt)
  }

  const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
  db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
  db.close()
  return dbPath
}

async function setupPostgres(pgConfig: pg.PoolConfig): Promise<void> {
  const pool = new pg.Pool({ ...pgConfig, max: 2 })

  try {
    for (const stmt of microSchemaPostgres
      .split(';')
      .map(s => s.trim())
      .filter(Boolean)) {
      const match = stmt.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i)
      if (match) await pool.query(`DROP TABLE IF EXISTS ${match[1]} CASCADE`)
      await pool.query(stmt)
    }

    const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
    const CHUNK = 500
    for (let offset = 0; offset < rows.length; offset += CHUNK) {
      const chunk = rows.slice(offset, offset + CHUNK)
      const values: unknown[] = []
      const placeholders: string[] = []
      for (let i = 0; i < chunk.length; i++) {
        const row = chunk[i]
        const rowPh: string[] = []
        for (let j = 0; j < 5; j++) {
          values.push(row[j])
          rowPh.push(`$${i * 5 + j + 1}`)
        }
        placeholders.push(`(${rowPh.join(', ')})`)
      }
      await pool.query(`INSERT INTO users (id, name, email, age, bio) VALUES ${placeholders.join(', ')}`, values)
    }
  } finally {
    await pool.end()
  }
}

function workloadLabel(readRatio: number): string {
  if (readRatio === 1.0) return 'read-only'
  if (readRatio === 0.5) return 'mixed-50/50'
  return `read-${Math.round(readRatio * 100)}%`
}

function pad(str: string, width: number, align: 'left' | 'right' = 'right'): string {
  return align === 'left' ? str.padEnd(width) : str.padStart(width)
}

function fmtOps(ops: number): string {
  if (ops >= 1_000_000) return `${(ops / 1_000_000).toFixed(2)}M`
  if (ops >= 1_000) return `${(ops / 1_000).toFixed(2)}K`
  return ops.toFixed(0)
}

function fmtLatency(ns: number): string {
  if (ns < 1_000) return `${ns.toFixed(0)} ns`
  if (ns < 1_000_000) return `${(ns / 1_000).toFixed(1)} us`
  return `${(ns / 1_000_000).toFixed(2)} ms`
}

function printScalingTable(title: string, dataPoints: ScalingDataPoint[]) {
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

  for (const dp of dataPoints) {
    console.log(
      [
        pad(workloadLabel(dp.readRatio), 14, 'left'),
        pad(String(dp.concurrency), 4),
        pad(fmtOps(dp.sirannonOps), 16),
        pad(fmtOps(dp.postgresOps), 16),
        pad(`${dp.speedup.toFixed(2)}x`, 10),
        pad(fmtLatency(dp.sirannonP50Ns), 12),
        pad(fmtLatency(dp.sirannonP99Ns), 12),
        pad(fmtLatency(dp.postgresP50Ns), 12),
        pad(fmtLatency(dp.postgresP99Ns), 12),
      ].join(' | '),
    )
  }

  console.log('-'.repeat(header.length))
}

function toComparisonResults(dataPoints: ScalingDataPoint[]): ComparisonResult[] {
  return dataPoints.map(dp => {
    const makeBenchResult = (label: string, opsPerSec: number, p50Ns: number, p99Ns: number): BenchmarkResult => ({
      name: label,
      opsPerSec,
      meanNs: 0,
      p50Ns,
      p75Ns: 0,
      p99Ns,
      p999Ns: 0,
      minNs: 0,
      maxNs: 0,
      sdNs: 0,
      cv: 0,
      moe: 0,
      samples: 0,
    })

    const wl = workloadLabel(dp.readRatio)
    return {
      workload: `${dp.model}/${wl}/n${dp.concurrency}`,
      dataSize: DATA_SIZE,
      sirannon: makeBenchResult(
        `sirannon-${wl}-n${dp.concurrency}`,
        dp.sirannonOps,
        dp.sirannonP50Ns,
        dp.sirannonP99Ns,
      ),
      postgres: makeBenchResult(
        `postgres-${wl}-n${dp.concurrency}`,
        dp.postgresOps,
        dp.postgresP50Ns,
        dp.postgresP99Ns,
      ),
      speedup: dp.speedup,
      framing: FRAMING,
    }
  })
}

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping concurrency scaling benchmark.')
    process.exit(0)
  }

  resetGlobalRng()
  const systemInfo = collectSystemInfo()

  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-scaling-'))
  const pgConfig: pg.PoolConfig = {
    host: config.postgres.host,
    port: config.postgres.port,
    user: config.postgres.user,
    password: config.postgres.password,
    database: config.postgres.database,
  }

  try {
    console.log('Setting up databases...')
    const dbPath = setupSirannonDb(tempDir)
    await setupPostgres(pgConfig)

    console.log(`Duration per test: ${DURATION_MS}ms`)
    console.log(`Data size: ${DATA_SIZE} rows`)
    console.log(`Concurrency levels: ${CONCURRENCY_LEVELS.join(', ')}`)
    console.log(`Read ratios: ${READ_RATIOS.join(', ')}`)

    const eventLoopPoints: ScalingDataPoint[] = []
    const workerThreadPoints: ScalingDataPoint[] = []

    for (const readRatio of READ_RATIOS) {
      const wl = workloadLabel(readRatio)

      for (const N of CONCURRENCY_LEVELS) {
        console.log(`\n--- Event loop: ${wl}, concurrency=${N} ---`)

        const sResult = runEventLoopSirannon(dbPath, N, readRatio)
        const sOpsPerSec = sResult.totalOps / (DURATION_MS / 1_000)

        const pResult = await runEventLoopPostgres(pgConfig, N, readRatio)
        const pOpsPerSec = pResult.totalOps / (DURATION_MS / 1_000)

        const speedup = pOpsPerSec > 0 ? sOpsPerSec / pOpsPerSec : Infinity

        eventLoopPoints.push({
          concurrency: N,
          readRatio,
          model: 'event-loop',
          sirannonOps: sOpsPerSec,
          postgresOps: pOpsPerSec,
          sirannonP50Ns: sResult.p50Ns,
          sirannonP99Ns: sResult.p99Ns,
          postgresP50Ns: pResult.p50Ns,
          postgresP99Ns: pResult.p99Ns,
          speedup,
        })

        console.log(
          `  Sirannon: ${fmtOps(sOpsPerSec)} ops/s | Postgres: ${fmtOps(pOpsPerSec)} ops/s | ${speedup.toFixed(2)}x`,
        )

        global.gc?.()
      }
    }

    for (const readRatio of READ_RATIOS) {
      const wl = workloadLabel(readRatio)

      for (const N of CONCURRENCY_LEVELS) {
        console.log(`\n--- Worker threads: ${wl}, concurrency=${N} ---`)

        const sResult = await runWorkerThreads('sirannon', N, readRatio, dbPath, pgConfig)
        const sOpsPerSec = sResult.totalOps / (DURATION_MS / 1_000)

        const pResult = await runWorkerThreads('postgres', N, readRatio, dbPath, pgConfig)
        const pOpsPerSec = pResult.totalOps / (DURATION_MS / 1_000)

        const speedup = pOpsPerSec > 0 ? sOpsPerSec / pOpsPerSec : Infinity

        workerThreadPoints.push({
          concurrency: N,
          readRatio,
          model: 'worker-threads',
          sirannonOps: sOpsPerSec,
          postgresOps: pOpsPerSec,
          sirannonP50Ns: sResult.p50Ns,
          sirannonP99Ns: sResult.p99Ns,
          postgresP50Ns: pResult.p50Ns,
          postgresP99Ns: pResult.p99Ns,
          speedup,
        })

        console.log(
          `  Sirannon: ${fmtOps(sOpsPerSec)} ops/s | Postgres: ${fmtOps(pOpsPerSec)} ops/s | ${speedup.toFixed(2)}x`,
        )

        global.gc?.()
      }
    }

    printScalingTable('Single Event Loop Model', eventLoopPoints)
    printScalingTable('Worker Thread Pool Model', workerThreadPoints)

    const allPoints = [...eventLoopPoints, ...workerThreadPoints]
    const results = toComparisonResults(allPoints)
    writeResults('concurrency-scaling', systemInfo, results)
  } finally {
    rmSync(tempDir, { recursive: true, force: true })
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
