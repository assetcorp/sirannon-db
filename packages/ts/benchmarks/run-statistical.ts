import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { loadConfig } from './config'
import { isPostgresAvailable } from './postgres-engine'

const BENCHMARKS = [
  { name: 'micro/point-select', path: 'benchmarks/micro/point-select.ts' },
  { name: 'micro/batch-update', path: 'benchmarks/micro/batch-update.ts' },
  { name: 'ycsb/workload-a', path: 'benchmarks/ycsb/workload-a.ts' },
  { name: 'oltp/tpc-c-lite', path: 'benchmarks/oltp/tpc-c-lite.ts' },
]

const RUNS = Number(process.env.BENCH_RUNS ?? 10)
const DATA_SIZES = process.env.BENCH_DATA_SIZES ?? '1000,10000'
const WARMUP_MS = process.env.BENCH_WARMUP_MS ?? '3000'
const MEASURE_MS = process.env.BENCH_MEASURE_MS ?? '8000'

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
    console.error('Postgres is required for statistical benchmarks.')
    console.error('Start Postgres with: docker compose -f benchmarks/docker-compose.yml up -d --wait')
    process.exit(1)
  }

  const estimatedMinutes = Math.ceil(
    (RUNS * BENCHMARKS.length * 2 * (Number(WARMUP_MS) + Number(MEASURE_MS) + 3000)) / 60_000,
  )
  console.log(`Statistical benchmark suite: ${BENCHMARKS.length} benchmarks x ${RUNS} runs`)
  console.log(`Data sizes: ${DATA_SIZES}`)
  console.log(`Warmup: ${WARMUP_MS}ms, Measure: ${MEASURE_MS}ms`)
  console.log(`Estimated runtime: ~${estimatedMinutes} minutes\n`)

  const passed: string[] = []
  const failed: string[] = []
  const suiteStart = performance.now()

  for (let idx = 0; idx < BENCHMARKS.length; idx++) {
    const bench = BENCHMARKS[idx]
    console.log(`\n${'='.repeat(60)}`)
    console.log(`[${idx + 1}/${BENCHMARKS.length}] Running ${bench.name} (${RUNS} runs)...`)
    console.log('='.repeat(60))

    const benchStart = performance.now()
    try {
      execFileSync('node', ['--expose-gc', '--import', 'tsx', bench.path], {
        cwd: resolve(import.meta.dirname, '..'),
        stdio: 'inherit',
        env: {
          ...process.env,
          BENCH_RUNS: String(RUNS),
          BENCH_DATA_SIZES: DATA_SIZES,
          BENCH_WARMUP_MS: WARMUP_MS,
          BENCH_MEASURE_MS: MEASURE_MS,
        },
        timeout: 1_200_000,
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
  console.log('STATISTICAL BENCHMARK SUMMARY')
  console.log('='.repeat(60))
  console.log(`Passed:  ${passed.length}`)
  console.log(`Failed:  ${failed.length}`)
  console.log(`Runs:    ${RUNS} per benchmark`)
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
