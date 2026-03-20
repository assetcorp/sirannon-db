import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { loadConfig } from './config'
import { isPostgresAvailable } from './postgres-engine'

interface BenchmarkEntry {
  name: string
  path: string
  requiresPostgres: boolean
}

const benchmarks: BenchmarkEntry[] = [
  { name: 'micro/point-select', path: 'benchmarks/micro/point-select.ts', requiresPostgres: true },
  { name: 'micro/bulk-insert', path: 'benchmarks/micro/bulk-insert.ts', requiresPostgres: true },
  { name: 'micro/batch-update', path: 'benchmarks/micro/batch-update.ts', requiresPostgres: true },
  { name: 'ycsb/workload-a', path: 'benchmarks/ycsb/workload-a.ts', requiresPostgres: true },
  { name: 'oltp/tpc-c-lite', path: 'benchmarks/oltp/tpc-c-lite.ts', requiresPostgres: true },
  { name: 'sirannon/cdc-latency', path: 'benchmarks/sirannon/cdc-latency.ts', requiresPostgres: false },
  { name: 'sirannon/connection-pool', path: 'benchmarks/sirannon/connection-pool.ts', requiresPostgres: false },
  { name: 'sirannon/cold-start', path: 'benchmarks/sirannon/cold-start.ts', requiresPostgres: false },
  { name: 'sirannon/multi-tenant', path: 'benchmarks/sirannon/multi-tenant.ts', requiresPostgres: false },
  { name: 'scaling/concurrency', path: 'benchmarks/scaling/concurrency.ts', requiresPostgres: true },
  { name: 'scaling/pool-sweep', path: 'benchmarks/scaling/pool-sweep.ts', requiresPostgres: true },
]

function fisherYatesShuffle<T>(arr: T[]): T[] {
  const result = [...arr]
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[result[i], result[j]] = [result[j], result[i]]
  }
  return result
}

function formatElapsed(ms: number): string {
  if (ms < 1_000) return `${ms.toFixed(0)}ms`
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`
  const minutes = Math.floor(ms / 60_000)
  const seconds = ((ms % 60_000) / 1_000).toFixed(0)
  return `${minutes}m${seconds}s`
}

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('WARNING: Postgres is not available. Benchmarks requiring Postgres will be skipped.')
    console.log('Start Postgres with: docker compose -f benchmarks/docker-compose.yml up -d --wait\n')
  }

  const shouldShuffle = (process.env.BENCH_SHUFFLE ?? 'true') !== 'false'
  const ordered = shouldShuffle ? fisherYatesShuffle(benchmarks) : benchmarks

  if (shouldShuffle) {
    console.log('Benchmark execution order (shuffled):')
  } else {
    console.log('Benchmark execution order (fixed):')
  }
  for (let i = 0; i < ordered.length; i++) {
    console.log(`  ${i + 1}. ${ordered[i].name}`)
  }
  console.log('')

  const passed: string[] = []
  const failed: string[] = []
  const skipped: string[] = []
  const suiteStart = performance.now()

  for (let idx = 0; idx < ordered.length; idx++) {
    const bench = ordered[idx]
    if (bench.requiresPostgres && !pgAvailable) {
      console.log(`[${idx + 1}/${ordered.length}] [SKIP] ${bench.name} (requires Postgres)`)
      skipped.push(bench.name)
      continue
    }

    console.log(`\n${'='.repeat(60)}`)
    console.log(`[${idx + 1}/${ordered.length}] Running ${bench.name}...`)
    console.log('='.repeat(60))

    const benchStart = performance.now()
    try {
      execFileSync('node', ['--expose-gc', '--import', 'tsx', bench.path], {
        cwd: resolve(import.meta.dirname, '..'),
        stdio: 'inherit',
        env: { ...process.env },
        timeout: 600_000,
      })
      const elapsed = performance.now() - benchStart
      console.log(`[PASS] ${bench.name} (${formatElapsed(elapsed)})`)
      passed.push(bench.name)
    } catch (_err) {
      const elapsed = performance.now() - benchStart
      console.error(`[FAIL] ${bench.name} (${formatElapsed(elapsed)})`)
      failed.push(bench.name)
    }
  }

  const totalElapsed = performance.now() - suiteStart

  console.log(`\n${'='.repeat(60)}`)
  console.log('BENCHMARK SUITE SUMMARY')
  console.log('='.repeat(60))
  console.log(`Passed:  ${passed.length}`)
  console.log(`Failed:  ${failed.length}`)
  console.log(`Skipped: ${skipped.length}`)
  console.log(`Total:   ${formatElapsed(totalElapsed)}`)

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
