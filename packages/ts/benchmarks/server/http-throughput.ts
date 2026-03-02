import { mkdtempSync, rmSync } from 'node:fs'
import http from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import autocannon from 'autocannon'
import pg from 'pg'
import { Sirannon } from '../../src/core/sirannon'
import { createServer } from '../../src/server/server'
import { collectSystemInfo, loadConfig } from '../config'
import { isPostgresAvailable } from '../postgres-engine'
import { type SirannonOnlyResult, writeSirannonOnlyResults } from '../reporter'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite } from '../schemas'

const FRAMING =
  'HTTP request throughput through the full stack (HTTP server + database). ' +
  'Tests end-to-end latency including serialization, routing, query execution, and response encoding. ' +
  'Sirannon uses uWebSockets.js; comparison uses Node http.createServer. ' +
  'Does not test WebSocket performance or streaming.'

const CONCURRENCY_LEVELS = [10, 50, 100, 200]
const DURATION = 10
const DATA_SIZE = 1_000

async function seedPostgres(pool: pg.Pool) {
  for (const stmt of microSchemaPostgres
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)) {
    await pool.query(stmt)
  }
  const colCount = 5
  const chunkSize = 500
  const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))

  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const chunk = rows.slice(offset, offset + chunkSize)
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

    await pool.query(`INSERT INTO users (id, name, email, age, bio) VALUES ${placeholders.join(', ')}`, values)
  }
}

function createPostgresServer(pool: pg.Pool): http.Server {
  return http.createServer(async (req, res) => {
    if (req.method !== 'POST') {
      res.writeHead(404, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: 'Not found' }))
      return
    }

    const chunks: Buffer[] = []
    for await (const chunk of req) {
      chunks.push(chunk as Buffer)
    }
    const body = JSON.parse(Buffer.concat(chunks).toString('utf-8'))

    try {
      const result = await pool.query({
        name: 'http-bench-query',
        text: body.sql.replace(/\?/g, (_match: string, offset: number, str: string) => {
          let idx = 0
          let count = 0
          while (idx <= offset) {
            if (str[idx] === '?') count++
            idx++
          }
          return `$${count}`
        }),
        values: body.params,
      })
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ rows: result.rows }))
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: String(err) }))
    }
  })
}

function runAutocannon(url: string, concurrency: number): Promise<autocannon.Result> {
  return new Promise((resolve, reject) => {
    const _instance = autocannon(
      {
        url,
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        connections: concurrency,
        duration: DURATION,
        pipelining: 1,
        setupClient(client) {
          const id = Math.floor(Math.random() * DATA_SIZE) + 1
          client.setBody(
            JSON.stringify({
              sql: 'SELECT * FROM users WHERE id = ?',
              params: [id],
            }),
          )
        },
      },
      (err, result) => {
        if (err) reject(err)
        else resolve(result)
      },
    )
  })
}

async function main() {
  const config = loadConfig()
  const systemInfo = collectSystemInfo()
  const results: SirannonOnlyResult[] = []

  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-bench-'))
  const dbPath = join(tempDir, 'http-bench.db')

  const sirannon = new Sirannon()
  const db = sirannon.open('bench', dbPath)

  for (const stmt of microSchemaSqlite
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)) {
    db.execute(stmt)
  }
  const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
  db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)

  const server = createServer(sirannon, { port: 0 })
  await server.listen()
  const sirannonPort = server.listeningPort
  const sirannonUrl = `http://127.0.0.1:${sirannonPort}/db/bench/query`

  console.log(`\nSirannon server listening on port ${sirannonPort}`)

  for (const concurrency of CONCURRENCY_LEVELS) {
    console.log(`  Running autocannon: ${concurrency} connections, ${DURATION}s...`)
    const result = await runAutocannon(sirannonUrl, concurrency)
    results.push({
      workload: `sirannon-http-c${concurrency}`,
      framing: FRAMING,
      result: {
        name: `sirannon-http-c${concurrency}`,
        opsPerSec: result.requests.average,
        meanNs: result.latency.mean * 1_000_000,
        p50Ns: result.latency.p50 * 1_000_000,
        p75Ns: result.latency.p75 * 1_000_000,
        p99Ns: result.latency.p99 * 1_000_000,
        p999Ns: 0,
        minNs: result.latency.min * 1_000_000,
        maxNs: result.latency.max * 1_000_000,
        sdNs: 0,
        cv: 0,
        moe: 0,
        samples: result.requests.total,
      },
    })
    console.log(
      `    ${result.requests.average.toFixed(0)} req/s, P50: ${result.latency.p50}ms, P99: ${result.latency.p99}ms`,
    )
  }

  await server.close()
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })

  const pgAvailable = await isPostgresAvailable(config)

  if (pgAvailable) {
    const pool = new pg.Pool({
      host: config.postgres.host,
      port: config.postgres.port,
      user: config.postgres.user,
      password: config.postgres.password,
      database: config.postgres.database,
      max: config.postgres.max,
    })

    try {
      await pool.query('DROP TABLE IF EXISTS users CASCADE')
    } catch {
      /* may not exist */
    }
    await seedPostgres(pool)

    const pgServer = createPostgresServer(pool)
    const pgPort = await new Promise<number>(resolve => {
      pgServer.listen(0, '127.0.0.1', () => {
        const addr = pgServer.address()
        resolve(typeof addr === 'object' && addr ? addr.port : 0)
      })
    })
    const pgUrl = `http://127.0.0.1:${pgPort}/query`

    console.log(`\nPostgres comparison server on port ${pgPort}`)

    for (const concurrency of CONCURRENCY_LEVELS) {
      console.log(`  Running autocannon: ${concurrency} connections, ${DURATION}s...`)
      const result = await runAutocannon(pgUrl, concurrency)
      results.push({
        workload: `postgres-http-c${concurrency}`,
        framing: FRAMING,
        result: {
          name: `postgres-http-c${concurrency}`,
          opsPerSec: result.requests.average,
          meanNs: result.latency.mean * 1_000_000,
          p50Ns: result.latency.p50 * 1_000_000,
          p75Ns: result.latency.p75 * 1_000_000,
          p99Ns: result.latency.p99 * 1_000_000,
          p999Ns: 0,
          minNs: result.latency.min * 1_000_000,
          maxNs: result.latency.max * 1_000_000,
          sdNs: 0,
          cv: 0,
          moe: 0,
          samples: result.requests.total,
        },
      })
      console.log(
        `    ${result.requests.average.toFixed(0)} req/s, P50: ${result.latency.p50}ms, P99: ${result.latency.p99}ms`,
      )
    }

    await new Promise<void>(resolve => pgServer.close(() => resolve()))
    await pool.query('DROP TABLE IF EXISTS users CASCADE')
    await pool.end()
  } else {
    console.log('\nPostgres not available, skipping HTTP comparison.')
  }

  writeSirannonOnlyResults('http-throughput', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
