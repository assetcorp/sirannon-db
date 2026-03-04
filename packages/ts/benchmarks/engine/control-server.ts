import { mkdtempSync, rmSync } from 'node:fs'
import http from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'
import { Bench } from 'tinybench'
import { SeededRng } from '../rng'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'
import { getWorkload, type WorkloadConfig, workloads } from './workloads'

const ENGINE = process.env.ENGINE ?? 'sirannon'
const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9878)
const DURABILITY = (process.env.BENCH_DURABILITY ?? 'matched') as 'matched' | 'full'

interface EngineBackend {
  setup(schemaSql: string): Promise<void>
  seed(insertSql: string, rows: unknown[][]): Promise<void>
  query(sql: string, params: unknown[]): unknown[]
  execute(sql: string, params: unknown[]): { changes: number }
  cleanup(): Promise<void>
  info(): Promise<Record<string, string>>
}

async function createSirannonBackend(): Promise<EngineBackend> {
  const { Database } = await import('../../src/core/database')
  let db: InstanceType<typeof Database> | null = null
  let tempDir = ''

  return {
    async setup(schemaSql: string) {
      tempDir = mkdtempSync(join(tmpdir(), 'sirannon-engine-'))
      const dbPath = join(tempDir, 'bench.db')
      db = new Database('bench', dbPath, { readPoolSize: 4, walMode: true })
      db.execute(DURABILITY === 'full' ? 'PRAGMA synchronous = FULL' : 'PRAGMA synchronous = NORMAL')

      for (const stmt of schemaSql
        .split(';')
        .map(s => s.trim())
        .filter(Boolean)) {
        db.execute(stmt)
      }
    },

    async seed(insertSql: string, rows: unknown[][]) {
      if (!db) throw new Error('Database not initialized')
      db.executeBatch(insertSql, rows)
    },

    query(sql: string, params: unknown[]) {
      if (!db) throw new Error('Database not initialized')
      return db.query(sql, params)
    },

    execute(sql: string, params: unknown[]) {
      if (!db) throw new Error('Database not initialized')
      return db.execute(sql, params)
    },

    async cleanup() {
      if (db && !db.closed) db.close()
      if (tempDir) rmSync(tempDir, { recursive: true, force: true })
      db = null
      tempDir = ''
    },

    async info() {
      if (!db) return { engine: 'sirannon', status: 'not initialized' }
      const pragmas = ['journal_mode', 'synchronous', 'cache_size', 'page_size']
      const result: Record<string, string> = { engine: 'sirannon' }
      for (const p of pragmas) {
        const rows = db.query<Record<string, unknown>>(`PRAGMA ${p}`)
        result[p] = String(rows[0] ? Object.values(rows[0])[0] : 'unknown')
      }
      return result
    },
  }
}

async function createPostgresBackend(): Promise<EngineBackend> {
  const pg = await import('pg')
  let pool: InstanceType<typeof pg.default.Pool> | null = null

  return {
    async setup(schemaSql: string) {
      pool = new pg.default.Pool({
        host: process.env.PGHOST ?? '127.0.0.1',
        port: Number(process.env.PGPORT ?? 5432),
        user: process.env.PGUSER ?? 'benchmark',
        password: process.env.PGPASSWORD ?? 'benchmark',
        database: process.env.PGDATABASE ?? 'benchmark',
        max: Number(process.env.PG_POOL_SIZE ?? 10),
      })

      const syncCommit = DURABILITY === 'full' ? 'on' : 'off'
      pool.on('connect', client => {
        client.query(`SET synchronous_commit = ${syncCommit}`)
      })

      for (const stmt of schemaSql
        .split(';')
        .map(s => s.trim())
        .filter(Boolean)) {
        const match = stmt.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i)
        if (match) {
          await pool.query(`DROP TABLE IF EXISTS ${match[1]} CASCADE`)
        }
        await pool.query(stmt)
      }
    },

    async seed(insertSql: string, rows: unknown[][]) {
      if (!pool || rows.length === 0) return
      const colCount = rows[0].length
      const CHUNK = 500

      for (let offset = 0; offset < rows.length; offset += CHUNK) {
        const chunk = rows.slice(offset, offset + CHUNK)
        const values: unknown[] = []
        const placeholders: string[] = []

        for (let i = 0; i < chunk.length; i++) {
          const row = chunk[i]
          const rowPh: string[] = []
          for (let j = 0; j < colCount; j++) {
            values.push(row[j])
            rowPh.push(`$${i * colCount + j + 1}`)
          }
          placeholders.push(`(${rowPh.join(', ')})`)
        }

        const baseSql = insertSql.replace(/VALUES\s*\(.*\)/i, '')
        const pgSql = `${baseSql} VALUES ${placeholders.join(', ')}`
        await pool.query(pgSql, values)
      }
    },

    query(_sql: string, _params: unknown[]) {
      throw new Error('Use async operations for Postgres')
    },

    execute(_sql: string, _params: unknown[]) {
      throw new Error('Use async operations for Postgres')
    },

    async cleanup() {
      if (pool) {
        const tables = ['order_items', 'orders', 'products', 'customers', 'usertable', 'users']
        for (const table of tables) {
          try {
            await pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
          } catch {}
        }
        await pool.end()
        pool = null
      }
    },

    async info() {
      if (!pool) return { engine: 'postgres', status: 'not initialized' }
      const settings = ['shared_buffers', 'work_mem', 'synchronous_commit', 'effective_cache_size']
      const result: Record<string, string> = { engine: 'postgres' }
      for (const s of settings) {
        const res = await pool.query(`SHOW ${s}`)
        result[s] = res.rows[0]?.[s] ?? 'unknown'
      }
      const ver = await pool.query('SELECT version()')
      result.version = ver.rows[0]?.version ?? 'unknown'
      return result
    },
  }
}

interface BenchmarkResultEntry {
  name: string
  opsPerSec: number
  meanNs: number
  p50Ns: number
  p75Ns: number
  p99Ns: number
  p999Ns: number
  samples: number
  cv: number
}

const MS_TO_NS = 1_000_000

async function runWorkloadBenchmark(backend: EngineBackend, config: WorkloadConfig): Promise<BenchmarkResultEntry[]> {
  const workload = getWorkload(config.name)
  if (!workload) throw new Error(`Unknown workload: ${config.name}`)

  const schemaSql = ENGINE === 'sirannon' ? workload.sqliteSchema : workload.postgresSchema
  await backend.setup(schemaSql)

  const seedData = workload.seedFn(config.dataSize)
  for (const { insertSql, rows } of seedData) {
    await backend.seed(insertSql, rows)
  }

  const bench = new Bench({
    warmupTime: config.warmupMs,
    time: config.measureMs,
    throws: true,
  })

  if (ENGINE === 'sirannon') {
    for (const op of workload.operations) {
      bench.add(`${config.name}/${op.name}`, () => {
        const params = op.paramsFn(config.dataSize)
        if (op.sqliteSql.trimStart().toUpperCase().startsWith('SELECT')) {
          backend.query(op.sqliteSql, params)
        } else {
          backend.execute(op.sqliteSql, params)
        }
      })
    }
  } else {
    const pg = await import('pg')
    const pool = new pg.default.Pool({
      host: process.env.PGHOST ?? '127.0.0.1',
      port: Number(process.env.PGPORT ?? 5432),
      user: process.env.PGUSER ?? 'benchmark',
      password: process.env.PGPASSWORD ?? 'benchmark',
      database: process.env.PGDATABASE ?? 'benchmark',
      max: Number(process.env.PG_POOL_SIZE ?? 10),
    })

    for (const op of workload.operations) {
      bench.add(
        `${config.name}/${op.name}`,
        async () => {
          const params = op.paramsFn(config.dataSize)
          await pool.query({ text: op.postgresSql, values: params })
        },
        { async: true },
      )
    }

    const originalRun = bench.run
    bench.run = async () => {
      const result = await originalRun.call(bench)
      await pool.end()
      return result
    }
  }

  const tasks = await bench.run()

  const results: BenchmarkResultEntry[] = tasks.map(task => {
    const r = task.result
    if (!r || r.state !== 'completed') {
      throw new Error(`Task '${task.name}' did not complete (state: ${r?.state ?? 'unknown'})`)
    }
    const lat = r.latency
    return {
      name: task.name,
      opsPerSec: r.throughput.mean,
      meanNs: lat.mean * MS_TO_NS,
      p50Ns: lat.p50 * MS_TO_NS,
      p75Ns: lat.p75 * MS_TO_NS,
      p99Ns: lat.p99 * MS_TO_NS,
      p999Ns: lat.p999 * MS_TO_NS,
      samples: lat.samplesCount,
      cv: lat.mean > 0 ? lat.sd / lat.mean : 0,
    }
  })

  await backend.cleanup()
  return results
}

interface ConcurrentConfig {
  concurrencyLevels: number[]
  durationMs: number
  dataSize: number
  readRatios: number[]
}

interface WorkerResult {
  ops: number
  latencySamplesNs: number[]
}

interface ConcurrentDataPoint {
  concurrency: number
  readRatio: number
  model: string
  totalOps: number
  opsPerSec: number
  p50Ns: number
  p99Ns: number
}

const WORKER_PATH = join(import.meta.dirname, '..', 'scaling', 'worker-bootstrap.mjs')
const BASE_SEED = 42

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const idx = Math.ceil(p * sorted.length) - 1
  return sorted[Math.max(0, idx)]
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

async function runConcurrentBenchmark(config: ConcurrentConfig): Promise<ConcurrentDataPoint[]> {
  const results: ConcurrentDataPoint[] = []

  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-conc-'))
  let sirannonDbPath = ''
  const pgConfig =
    ENGINE === 'postgres'
      ? {
          host: process.env.PGHOST ?? '127.0.0.1',
          port: Number(process.env.PGPORT ?? 5432),
          user: process.env.PGUSER ?? 'benchmark',
          password: process.env.PGPASSWORD ?? 'benchmark',
          database: process.env.PGDATABASE ?? 'benchmark',
        }
      : undefined

  try {
    if (ENGINE === 'sirannon') {
      const { Database } = await import('../../src/core/database')
      sirannonDbPath = join(tempDir, 'conc.db')
      const db = new Database('conc-setup', sirannonDbPath, { readPoolSize: 1, walMode: true })
      db.execute(DURABILITY === 'full' ? 'PRAGMA synchronous = FULL' : 'PRAGMA synchronous = NORMAL')

      try {
        for (const stmt of microSchemaSqlite
          .split(';')
          .map(s => s.trim())
          .filter(Boolean)) {
          db.execute(stmt)
        }
        const rows = Array.from({ length: config.dataSize }, (_, i) => generateUserRow(i + 1))
        db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
      } finally {
        db.close()
      }
    } else {
      const pg = await import('pg')
      const pool = new pg.default.Pool({ ...pgConfig, max: 2 })
      const syncCommitConc = DURABILITY === 'full' ? 'on' : 'off'
      pool.on('connect', client => {
        client.query(`SET synchronous_commit = ${syncCommitConc}`)
      })

      try {
        for (const stmt of microSchemaPostgres
          .split(';')
          .map(s => s.trim())
          .filter(Boolean)) {
          const match = stmt.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i)
          if (match) await pool.query(`DROP TABLE IF EXISTS ${match[1]} CASCADE`)
          await pool.query(stmt)
        }

        const rows = Array.from({ length: config.dataSize }, (_, i) => generateUserRow(i + 1))
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

    for (const readRatio of config.readRatios) {
      for (const N of config.concurrencyLevels) {
        console.log(`[${ENGINE}] Event loop: readRatio=${readRatio}, N=${N}`)

        if (ENGINE === 'sirannon') {
          const { Database } = await import('../../src/core/database')
          const db = new Database('conc-el', sirannonDbPath, { readPoolSize: 1, walMode: true })
          db.execute('PRAGMA busy_timeout = 5000')
          db.execute(DURABILITY === 'full' ? 'PRAGMA synchronous = FULL' : 'PRAGMA synchronous = NORMAL')

          try {
            const rng = new SeededRng(BigInt(BASE_SEED))
            const zipf = new ZipfianGenerator(config.dataSize, 0.99, rng)

            let ops = 0
            const samples: number[] = []
            const deadline = Date.now() + config.durationMs

            while (Date.now() < deadline) {
              for (let i = 0; i < N; i++) {
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
                if (samples.length < 10_000) samples.push(elapsed)
              }
            }

            samples.sort((a, b) => a - b)
            const opsPerSec = ops / (config.durationMs / 1_000)
            results.push({
              concurrency: N,
              readRatio,
              model: 'event-loop',
              totalOps: ops,
              opsPerSec,
              p50Ns: percentile(samples, 0.5),
              p99Ns: percentile(samples, 0.99),
            })
          } finally {
            db.close()
          }
        } else {
          const pg = await import('pg')
          const pool = new pg.default.Pool({ ...pgConfig, max: N })
          const syncCommitEl = DURABILITY === 'full' ? 'on' : 'off'
          pool.on('connect', client => {
            client.query(`SET synchronous_commit = ${syncCommitEl}`)
          })

          try {
            const rng = new SeededRng(BigInt(BASE_SEED))
            const zipf = new ZipfianGenerator(config.dataSize, 0.99, rng)

            let ops = 0
            const samples: number[] = []
            const deadline = Date.now() + config.durationMs

            while (Date.now() < deadline) {
              const batch: Promise<void>[] = []
              for (let i = 0; i < N; i++) {
                const isRead = rng.next() < readRatio
                const id = zipf.next() + 1
                const start = process.hrtime.bigint()

                if (isRead) {
                  batch.push(
                    pool.query({ text: 'SELECT * FROM users WHERE id = $1', values: [id] }).then(() => {
                      const elapsed = Number(process.hrtime.bigint() - start)
                      ops++
                      if (samples.length < 10_000) samples.push(elapsed)
                    }),
                  )
                } else {
                  const age = Math.floor(rng.next() * 80) + 18
                  batch.push(
                    pool.query({ text: 'UPDATE users SET age = $1 WHERE id = $2', values: [age, id] }).then(() => {
                      const elapsed = Number(process.hrtime.bigint() - start)
                      ops++
                      if (samples.length < 10_000) samples.push(elapsed)
                    }),
                  )
                }
              }
              await Promise.all(batch)
            }

            samples.sort((a, b) => a - b)
            const opsPerSec = ops / (config.durationMs / 1_000)
            results.push({
              concurrency: N,
              readRatio,
              model: 'event-loop',
              totalOps: ops,
              opsPerSec,
              p50Ns: percentile(samples, 0.5),
              p99Ns: percentile(samples, 0.99),
            })
          } finally {
            await pool.end()
          }
        }

        console.log(`[${ENGINE}] Worker threads: readRatio=${readRatio}, N=${N}`)

        const workers: Promise<WorkerResult>[] = []
        for (let i = 0; i < N; i++) {
          const wConfig: Record<string, unknown> = {
            engine: ENGINE,
            durationMs: config.durationMs,
            dataSize: config.dataSize,
            readRatio,
            seed: BASE_SEED + i + 1,
            durability: DURABILITY,
          }
          if (ENGINE === 'sirannon') {
            wConfig.sirannonDbPath = sirannonDbPath
          } else {
            wConfig.pgConfig = pgConfig
          }
          workers.push(spawnWorker(wConfig))
        }

        const workerResults = await Promise.all(workers)
        const totalOps = workerResults.reduce((sum, r) => sum + r.ops, 0)
        const mergedSamples = workerResults.flatMap(r => r.latencySamplesNs).sort((a, b) => a - b)
        const opsPerSec = totalOps / (config.durationMs / 1_000)

        results.push({
          concurrency: N,
          readRatio,
          model: 'worker-threads',
          totalOps,
          opsPerSec,
          p50Ns: percentile(mergedSamples, 0.5),
          p99Ns: percentile(mergedSamples, 0.99),
        })
      }
    }
  } finally {
    rmSync(tempDir, { recursive: true, force: true })
  }

  return results
}

function sendJson(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(body))
}

async function readBody(req: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of req) {
    chunks.push(chunk as Buffer)
  }
  return Buffer.concat(chunks).toString('utf-8')
}

let backend: EngineBackend | null = null

async function initBackend(): Promise<EngineBackend> {
  if (backend) return backend
  backend = ENGINE === 'sirannon' ? await createSirannonBackend() : await createPostgresBackend()
  return backend
}

const server = http.createServer(async (req, res) => {
  const url = req.url ?? ''
  const method = req.method ?? ''

  try {
    if (url === '/health' && method === 'GET') {
      sendJson(res, 200, { status: 'ok', engine: ENGINE })
      return
    }

    if (url === '/info' && method === 'GET') {
      const b = await initBackend()
      const info = await b.info()
      sendJson(res, 200, info)
      return
    }

    if (url === '/benchmark' && method === 'POST') {
      const body = JSON.parse(await readBody(req))
      const config: WorkloadConfig = {
        name: body.name,
        dataSize: body.dataSize ?? 10_000,
        warmupMs: body.warmupMs ?? 5_000,
        measureMs: body.measureMs ?? 10_000,
      }

      const b = await initBackend()
      const results = await runWorkloadBenchmark(b, config)
      sendJson(res, 200, { engine: ENGINE, workload: config.name, dataSize: config.dataSize, results })
      return
    }

    if (url === '/benchmark/all' && method === 'POST') {
      const body = JSON.parse(await readBody(req))
      const dataSizes: number[] = body.dataSizes ?? [1_000, 10_000]
      const warmupMs = body.warmupMs ?? 5_000
      const measureMs = body.measureMs ?? 10_000
      const workloadNames: string[] = body.workloads ?? workloads.map(w => w.name)

      const b = await initBackend()
      const allResults: Record<string, unknown>[] = []

      for (const name of workloadNames) {
        for (const dataSize of dataSizes) {
          console.log(`[${ENGINE}] Running ${name} @ ${dataSize} rows...`)
          const results = await runWorkloadBenchmark(b, { name, dataSize, warmupMs, measureMs })
          allResults.push({ workload: name, dataSize, results })
        }
      }

      sendJson(res, 200, { engine: ENGINE, results: allResults })
      return
    }

    if (url === '/benchmark/concurrent' && method === 'POST') {
      const body = JSON.parse(await readBody(req))
      const concConfig: ConcurrentConfig = {
        concurrencyLevels: body.concurrencyLevels ?? [1, 2, 4, 8, 16, 32, 64],
        durationMs: body.durationMs ?? 10_000,
        dataSize: body.dataSize ?? 10_000,
        readRatios: body.readRatios ?? [1.0, 0.5],
      }

      const dataPoints = await runConcurrentBenchmark(concConfig)
      sendJson(res, 200, { engine: ENGINE, results: dataPoints })
      return
    }

    if (url === '/cleanup' && method === 'POST') {
      if (backend) {
        await backend.cleanup()
        backend = null
      }
      sendJson(res, 200, { status: 'cleaned' })
      return
    }

    sendJson(res, 404, { error: 'not found' })
  } catch (err) {
    console.error(`Error handling ${method} ${url}:`, err)
    sendJson(res, 500, { error: String(err) })
  }
})

server.requestTimeout = 0
server.headersTimeout = 0
server.timeout = 0

server.listen(Number(PORT), HOST, () => {
  console.log(`Engine control server (${ENGINE}) listening on ${HOST}:${PORT}`)
})

process.on('SIGTERM', async () => {
  if (backend) await backend.cleanup()
  server.close()
  process.exit(0)
})

process.on('SIGINT', async () => {
  if (backend) await backend.cleanup()
  server.close()
  process.exit(0)
})
