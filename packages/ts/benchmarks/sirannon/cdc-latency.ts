import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { Database } from '../../src/core/database'
import { collectSystemInfo } from '../config'
import { writeSirannonOnlyResults } from '../reporter'

const FRAMING =
  'End-to-end latency from INSERT to subscription callback with 1ms CDC poll interval. ' +
  'Dominated by poll interval. Measures trigger + poll + dispatch overhead. ' +
  'Does not test sustained CDC throughput under high write rates.'

const ITERATIONS = 1_000

async function main() {
  const systemInfo = collectSystemInfo()
  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cdc-bench-'))
  const dbPath = join(tempDir, 'cdc-bench.db')

  const db = new Database('cdc-bench', dbPath, {
    cdcPollInterval: 1,
    walMode: true,
  })

  db.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, data TEXT)')
  db.watch('events')

  const keepAlive = setInterval(() => {}, 60_000)

  const samples: number[] = []
  let pendingResolve: ((elapsed: number) => void) | null = null
  let insertTime = 0

  const subscription = db.on('events').subscribe(() => {
    if (pendingResolve) {
      const elapsed = performance.now() - insertTime
      const resolve = pendingResolve
      pendingResolve = null
      resolve(elapsed)
    }
  })

  for (let i = 0; i < ITERATIONS; i++) {
    const elapsed = await new Promise<number>(resolve => {
      pendingResolve = resolve
      insertTime = performance.now()
      db.execute('INSERT INTO events (data) VALUES (?)', [`payload-${i}`])
    })
    samples.push(elapsed)
  }

  subscription.unsubscribe()
  clearInterval(keepAlive)
  db.close()
  rmSync(tempDir, { recursive: true, force: true })

  samples.sort((a, b) => a - b)

  const mean = samples.reduce((sum, v) => sum + v, 0) / samples.length
  const p50 = samples[Math.floor(samples.length * 0.5)]
  const p95 = samples[Math.floor(samples.length * 0.95)]
  const p99 = samples[Math.floor(samples.length * 0.99)]
  const min = samples[0]
  const max = samples[samples.length - 1]

  const variance = samples.reduce((sum, v) => sum + (v - mean) ** 2, 0) / samples.length
  const sd = Math.sqrt(variance)
  const cv = mean > 0 ? sd / mean : 0

  console.log('\n=== CDC Latency Benchmark ===')
  console.log(`Iterations: ${ITERATIONS}`)
  console.log(`Mean:       ${mean.toFixed(3)} ms`)
  console.log(`P50:        ${p50.toFixed(3)} ms`)
  console.log(`P95:        ${p95.toFixed(3)} ms`)
  console.log(`P99:        ${p99.toFixed(3)} ms`)
  console.log(`Min:        ${min.toFixed(3)} ms`)
  console.log(`Max:        ${max.toFixed(3)} ms`)
  console.log(`SD:         ${sd.toFixed(3)} ms`)
  console.log(`CV:         ${(cv * 100).toFixed(1)}%${cv > 0.1 ? ' [!]' : ''}`)
  console.log('=============================\n')

  const msToNs = 1_000_000
  writeSirannonOnlyResults('cdc-latency', systemInfo, [
    {
      workload: 'cdc-insert-to-callback',
      framing: FRAMING,
      result: {
        name: 'cdc-latency',
        opsPerSec: 1000 / mean,
        meanNs: mean * msToNs,
        p50Ns: p50 * msToNs,
        p75Ns: samples[Math.floor(samples.length * 0.75)] * msToNs,
        p99Ns: p99 * msToNs,
        p999Ns: samples[Math.floor(samples.length * 0.999)] * msToNs,
        minNs: min * msToNs,
        maxNs: max * msToNs,
        sdNs: sd * msToNs,
        cv,
        moe: 0,
        samples: ITERATIONS,
      },
    },
  ])
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
