import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { loadConfig } from './config'
import { isPostgresAvailable } from './postgres-engine'

interface BenchmarkEntry {
  name: string
  path: string
  requiresPostgres: boolean
}

const benchmarks: BenchmarkEntry[] = [
  { name: 'micro/point-select', path: 'benchmarks/micro/point-select.ts', requiresPostgres: true },
  { name: 'micro/range-select', path: 'benchmarks/micro/range-select.ts', requiresPostgres: true },
  { name: 'micro/bulk-insert', path: 'benchmarks/micro/bulk-insert.ts', requiresPostgres: true },
  { name: 'micro/batch-update', path: 'benchmarks/micro/batch-update.ts', requiresPostgres: true },
  { name: 'ycsb/workload-a', path: 'benchmarks/ycsb/workload-a.ts', requiresPostgres: true },
  { name: 'ycsb/workload-b', path: 'benchmarks/ycsb/workload-b.ts', requiresPostgres: true },
  { name: 'ycsb/workload-c', path: 'benchmarks/ycsb/workload-c.ts', requiresPostgres: true },
  { name: 'oltp/tpc-c-lite', path: 'benchmarks/oltp/tpc-c-lite.ts', requiresPostgres: true },
  { name: 'sirannon/cdc-latency', path: 'benchmarks/sirannon/cdc-latency.ts', requiresPostgres: false },
  { name: 'sirannon/connection-pool', path: 'benchmarks/sirannon/connection-pool.ts', requiresPostgres: false },
  { name: 'sirannon/cold-start', path: 'benchmarks/sirannon/cold-start.ts', requiresPostgres: false },
  { name: 'sirannon/multi-tenant', path: 'benchmarks/sirannon/multi-tenant.ts', requiresPostgres: false },
  { name: 'server/http-throughput', path: 'benchmarks/server/http-throughput.ts', requiresPostgres: false },
]

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('WARNING: Postgres is not available. Benchmarks requiring Postgres will be skipped.')
    console.log('Start Postgres with: docker compose -f benchmarks/docker-compose.yml up -d --wait\n')
  }

  const passed: string[] = []
  const failed: string[] = []
  const skipped: string[] = []

  for (const bench of benchmarks) {
    if (bench.requiresPostgres && !pgAvailable) {
      console.log(`[SKIP] ${bench.name} (requires Postgres)`)
      skipped.push(bench.name)
      continue
    }

    console.log(`\n${'='.repeat(60)}`)
    console.log(`[RUN]  ${bench.name}`)
    console.log('='.repeat(60))

    try {
      execFileSync('node', ['--expose-gc', '--import', 'tsx', bench.path], {
        cwd: resolve(import.meta.dirname, '..'),
        stdio: 'inherit',
        env: { ...process.env },
        timeout: 600_000,
      })
      console.log(`[PASS] ${bench.name}`)
      passed.push(bench.name)
    } catch (_err) {
      console.error(`[FAIL] ${bench.name}`)
      failed.push(bench.name)
    }
  }

  console.log(`\n${'='.repeat(60)}`)
  console.log('BENCHMARK SUITE SUMMARY')
  console.log('='.repeat(60))
  console.log(`Passed:  ${passed.length}`)
  console.log(`Failed:  ${failed.length}`)
  console.log(`Skipped: ${skipped.length}`)

  if (failed.length > 0) {
    console.log('\nFailed benchmarks:')
    for (const name of failed) {
      console.log(`  - ${name}`)
    }
    process.exit(1)
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
