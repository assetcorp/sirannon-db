import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import pg from 'pg'
import { Database } from '../../src/core/database'
import { collectSystemInfo, loadConfig } from '../config'
import { isPostgresAvailable } from '../postgres-engine'
import { writeSirannonOnlyResults } from '../reporter'

const FRAMING =
  'Time from zero to first query result. Measures connection establishment overhead. ' +
  'Critical for serverless, edge functions, and CLI tools. ' +
  'Does not test steady-state query throughput.'

const ITERATIONS = 200

function computeStats(samples: number[]) {
  samples.sort((a, b) => a - b)
  const mean = samples.reduce((sum, v) => sum + v, 0) / samples.length
  const variance = samples.reduce((sum, v) => sum + (v - mean) ** 2, 0) / samples.length
  const sd = Math.sqrt(variance)
  return {
    mean,
    p50: samples[Math.floor(samples.length * 0.5)],
    p95: samples[Math.floor(samples.length * 0.95)],
    p99: samples[Math.floor(samples.length * 0.99)],
    min: samples[0],
    max: samples[samples.length - 1],
    sd,
    cv: mean > 0 ? sd / mean : 0,
  }
}

async function main() {
  const config = loadConfig()
  const systemInfo = collectSystemInfo()

  global.gc?.()

  const sirannonSamples: number[] = []
  for (let i = 0; i < ITERATIONS; i++) {
    const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cold-'))
    const dbPath = join(tempDir, 'cold.db')

    const start = performance.now()
    const db = new Database(`cold-${i}`, dbPath)
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    db.query('SELECT 1')
    db.close()
    const elapsed = performance.now() - start

    rmSync(tempDir, { recursive: true, force: true })
    sirannonSamples.push(elapsed)
  }

  const sirannonStats = computeStats(sirannonSamples)

  console.log('\n=== Cold Start Benchmark ===')
  console.log('\nSirannon (SQLite):')
  console.log(`  Mean:  ${sirannonStats.mean.toFixed(3)} ms`)
  console.log(`  P50:   ${sirannonStats.p50.toFixed(3)} ms`)
  console.log(`  P95:   ${sirannonStats.p95.toFixed(3)} ms`)
  console.log(`  P99:   ${sirannonStats.p99.toFixed(3)} ms`)
  console.log(`  Min:   ${sirannonStats.min.toFixed(3)} ms`)
  console.log(`  Max:   ${sirannonStats.max.toFixed(3)} ms`)

  const pgAvailable = await isPostgresAvailable(config)
  let postgresStats = null

  if (pgAvailable) {
    global.gc?.()

    const postgresSamples: number[] = []
    for (let i = 0; i < ITERATIONS; i++) {
      const client = new pg.Client({
        host: config.postgres.host,
        port: config.postgres.port,
        user: config.postgres.user,
        password: config.postgres.password,
        database: config.postgres.database,
      })

      const start = performance.now()
      await client.connect()
      await client.query('SELECT 1')
      await client.end()
      const elapsed = performance.now() - start

      postgresSamples.push(elapsed)
    }

    postgresStats = computeStats(postgresSamples)

    console.log('\nPostgres:')
    console.log(`  Mean:  ${postgresStats.mean.toFixed(3)} ms`)
    console.log(`  P50:   ${postgresStats.p50.toFixed(3)} ms`)
    console.log(`  P95:   ${postgresStats.p95.toFixed(3)} ms`)
    console.log(`  P99:   ${postgresStats.p99.toFixed(3)} ms`)
    console.log(`  Min:   ${postgresStats.min.toFixed(3)} ms`)
    console.log(`  Max:   ${postgresStats.max.toFixed(3)} ms`)

    const speedup = postgresStats.mean / sirannonStats.mean
    console.log(`\nSirannon cold start is ${speedup.toFixed(1)}x faster than Postgres`)
  } else {
    console.log('\nPostgres not available, skipping comparison.')
  }

  console.log('============================\n')

  const msToNs = 1_000_000
  const allResults = [
    {
      workload: 'cold-start-sirannon',
      framing: FRAMING,
      result: {
        name: 'cold-start-sirannon',
        opsPerSec: 1000 / sirannonStats.mean,
        meanNs: sirannonStats.mean * msToNs,
        p50Ns: sirannonStats.p50 * msToNs,
        p75Ns: sirannonSamples.sort((a, b) => a - b)[Math.floor(ITERATIONS * 0.75)] * msToNs,
        p99Ns: sirannonStats.p99 * msToNs,
        p999Ns: sirannonSamples[Math.floor(ITERATIONS * 0.999)] * msToNs,
        minNs: sirannonStats.min * msToNs,
        maxNs: sirannonStats.max * msToNs,
        sdNs: sirannonStats.sd * msToNs,
        cv: sirannonStats.cv,
        moe: 0,
        samples: ITERATIONS,
      },
    },
  ]

  if (postgresStats) {
    allResults.push({
      workload: 'cold-start-postgres',
      framing: FRAMING,
      result: {
        name: 'cold-start-postgres',
        opsPerSec: 1000 / postgresStats.mean,
        meanNs: postgresStats.mean * msToNs,
        p50Ns: postgresStats.p50 * msToNs,
        p75Ns: 0,
        p99Ns: postgresStats.p99 * msToNs,
        p999Ns: 0,
        minNs: postgresStats.min * msToNs,
        maxNs: postgresStats.max * msToNs,
        sdNs: postgresStats.sd * msToNs,
        cv: postgresStats.cv,
        moe: 0,
        samples: ITERATIONS,
      },
    })
  }

  writeSirannonOnlyResults('cold-start', systemInfo, allResults)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
