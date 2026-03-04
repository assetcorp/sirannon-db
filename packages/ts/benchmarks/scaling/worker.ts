import { parentPort, workerData } from 'node:worker_threads'
import { SeededRng } from '../rng'
import { ZipfianGenerator } from '../schemas'

interface WorkerConfig {
  engine: 'sirannon' | 'postgres'
  durationMs: number
  dataSize: number
  readRatio: number
  seed: number
  sirannonDbPath?: string
  pgConfig?: {
    host: string
    port: number
    user: string
    password: string
    database: string
  }
}

interface WorkerResult {
  ops: number
  latencySamplesNs: number[]
}

const config = workerData as WorkerConfig
const rng = new SeededRng(BigInt(config.seed))
const zipf = new ZipfianGenerator(config.dataSize, 0.99, rng)

const READ_SQL_SQLITE = 'SELECT * FROM users WHERE id = ?'
const WRITE_SQL_SQLITE = 'UPDATE users SET age = ? WHERE id = ?'
const READ_SQL_PG = 'SELECT * FROM users WHERE id = $1'
const WRITE_SQL_PG = 'UPDATE users SET age = $1 WHERE id = $2'

const MAX_LATENCY_SAMPLES = 10_000

function sampleLatency(samples: number[], value: number, ops: number) {
  if (samples.length < MAX_LATENCY_SAMPLES) {
    samples.push(value)
  } else {
    const idx = Math.floor(rng.next() * ops)
    if (idx < MAX_LATENCY_SAMPLES) {
      samples[idx] = value
    }
  }
}

async function runSirannon(): Promise<WorkerResult> {
  const { Database } = await import('../../src/core/database')
  const dbPath = config.sirannonDbPath ?? ''
  const db = new Database(`worker-${config.seed}`, dbPath, {
    readPoolSize: 1,
    walMode: true,
  })
  db.execute('PRAGMA busy_timeout = 5000')
  db.execute('PRAGMA synchronous = NORMAL')

  let ops = 0
  const latencySamplesNs: number[] = []
  const deadline = Date.now() + config.durationMs

  while (Date.now() < deadline) {
    const isRead = rng.next() < config.readRatio
    const id = zipf.next() + 1

    const start = process.hrtime.bigint()
    if (isRead) {
      db.query(READ_SQL_SQLITE, [id])
    } else {
      const age = Math.floor(rng.next() * 80) + 18
      db.execute(WRITE_SQL_SQLITE, [age, id])
    }
    const elapsed = Number(process.hrtime.bigint() - start)
    ops++
    sampleLatency(latencySamplesNs, elapsed, ops)
  }

  db.close()
  return { ops, latencySamplesNs }
}

async function runPostgres(): Promise<WorkerResult> {
  const pg = await import('pg')
  const pool = new pg.default.Pool({
    ...config.pgConfig,
    max: 1,
  })

  let ops = 0
  const latencySamplesNs: number[] = []
  const deadline = Date.now() + config.durationMs

  while (Date.now() < deadline) {
    const isRead = rng.next() < config.readRatio
    const id = zipf.next() + 1

    const start = process.hrtime.bigint()
    if (isRead) {
      await pool.query({ text: READ_SQL_PG, values: [id] })
    } else {
      const age = Math.floor(rng.next() * 80) + 18
      await pool.query({ text: WRITE_SQL_PG, values: [age, id] })
    }
    const elapsed = Number(process.hrtime.bigint() - start)
    ops++
    sampleLatency(latencySamplesNs, elapsed, ops)
  }

  await pool.end()
  return { ops, latencySamplesNs }
}

async function main() {
  if (!parentPort) throw new Error('worker.ts must run inside a Worker thread')
  const result = config.engine === 'sirannon' ? await runSirannon() : await runPostgres()
  parentPort.postMessage(result)
}

main().catch(err => {
  console.error(`Worker error (${config.engine}, seed=${config.seed}):`, err)
  process.exit(1)
})
