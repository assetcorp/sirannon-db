import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { Database } from '../../src/core/database'
import { collectSystemInfo } from '../config'
import { type SirannonOnlyResult, writeSirannonOnlyResults } from '../reporter'
import { generateUserRow, microSchemaSqlite } from '../schemas'

const FRAMING =
  'Multi-tenant database isolation with 50 databases, 100 users each. ' +
  'Tests per-database memory overhead and cross-database query latency. ' +
  'A use case where embedded SQLite has a structural advantage over client-server databases. ' +
  'Does not test concurrent cross-tenant access patterns.'

const NUM_DATABASES = 50
const USERS_PER_DB = 100

async function main() {
  const systemInfo = collectSystemInfo()
  const tempDirs: string[] = []
  const databases: Database[] = []

  global.gc?.()
  const rssBefore = process.memoryUsage().rss

  const openSamples: number[] = []

  for (let i = 0; i < NUM_DATABASES; i++) {
    const tempDir = mkdtempSync(join(tmpdir(), `sirannon-tenant-${i}-`))
    tempDirs.push(tempDir)
    const dbPath = join(tempDir, 'tenant.db')

    const start = performance.now()
    const db = new Database(`tenant-${i}`, dbPath, { readPoolSize: 1, walMode: true })

    for (const stmt of microSchemaSqlite
      .split(';')
      .map(s => s.trim())
      .filter(Boolean)) {
      db.execute(stmt)
    }

    const rows = Array.from({ length: USERS_PER_DB }, (_, j) => generateUserRow(j + 1))
    db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)

    const elapsed = performance.now() - start
    openSamples.push(elapsed)
    databases.push(db)
  }

  global.gc?.()
  const rssAfter = process.memoryUsage().rss
  const rssDelta = rssAfter - rssBefore
  const memoryPerDb = rssDelta / NUM_DATABASES

  const queryStart = performance.now()
  for (const db of databases) {
    db.query('SELECT * FROM users WHERE id = ?', [1])
  }
  const queryElapsed = performance.now() - queryStart

  const closeSamples: number[] = []
  for (const db of databases) {
    const start = performance.now()
    db.close()
    closeSamples.push(performance.now() - start)
  }

  for (const dir of tempDirs) {
    rmSync(dir, { recursive: true, force: true })
  }

  const avgOpenMs = openSamples.reduce((s, v) => s + v, 0) / openSamples.length
  const avgCloseMs = closeSamples.reduce((s, v) => s + v, 0) / closeSamples.length
  const avgQueryMs = queryElapsed / NUM_DATABASES

  console.log('\n=== Multi-Tenant Benchmark ===')
  console.log(`Databases:        ${NUM_DATABASES}`)
  console.log(`Users per DB:     ${USERS_PER_DB}`)
  console.log(`Avg open+seed:    ${avgOpenMs.toFixed(3)} ms`)
  console.log(`Avg close:        ${avgCloseMs.toFixed(3)} ms`)
  console.log(`Total query time: ${queryElapsed.toFixed(3)} ms (${NUM_DATABASES} queries)`)
  console.log(`Avg query:        ${avgQueryMs.toFixed(3)} ms`)
  console.log(`RSS delta:        ${(rssDelta / (1024 * 1024)).toFixed(1)} MB`)
  console.log(`Memory per DB:    ${(memoryPerDb / (1024 * 1024)).toFixed(2)} MB`)
  console.log('==============================\n')

  const msToNs = 1_000_000
  const results: SirannonOnlyResult[] = [
    {
      workload: 'multi-tenant-query',
      framing: FRAMING,
      result: {
        name: 'multi-tenant-query',
        opsPerSec: NUM_DATABASES / (queryElapsed / 1000),
        meanNs: avgQueryMs * msToNs,
        p50Ns: avgQueryMs * msToNs,
        p75Ns: 0,
        p99Ns: 0,
        p999Ns: 0,
        minNs: 0,
        maxNs: 0,
        sdNs: 0,
        cv: 0,
        moe: 0,
        samples: NUM_DATABASES,
      },
    },
    {
      workload: 'multi-tenant-open',
      framing: FRAMING,
      result: {
        name: 'multi-tenant-open',
        opsPerSec: 1000 / avgOpenMs,
        meanNs: avgOpenMs * msToNs,
        p50Ns: openSamples.sort((a, b) => a - b)[Math.floor(NUM_DATABASES * 0.5)] * msToNs,
        p75Ns: 0,
        p99Ns: 0,
        p999Ns: 0,
        minNs: openSamples[0] * msToNs,
        maxNs: openSamples[openSamples.length - 1] * msToNs,
        sdNs: 0,
        cv: 0,
        moe: 0,
        samples: NUM_DATABASES,
      },
    },
  ]

  writeSirannonOnlyResults('multi-tenant', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
