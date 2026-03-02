import { mkdtempSync, rmSync } from 'node:fs'
import http from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Bench } from 'tinybench'
import { getWorkload, type WorkloadConfig, workloads } from './workloads'

const ENGINE = process.env.ENGINE ?? 'sirannon'
const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9878)

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
      db.execute('PRAGMA synchronous = NORMAL')

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
