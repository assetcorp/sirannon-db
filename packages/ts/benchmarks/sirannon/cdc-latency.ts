import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { Database } from '../../src/core/database'
import { collectSystemInfo, loadBenchDriver, loadConfig } from '../config'
import { writeSirannonOnlyResults } from '../reporter'

const FRAMING =
  'CDC throughput: measures sustained event delivery rate and lag under continuous writes. ' +
  'Inserts rows at maximum speed for the measurement duration, then tracks how many CDC ' +
  'events are delivered, the per-event delivery lag (insert timestamp to callback), and ' +
  'the delivery ratio (events received / rows inserted). Uses 1ms CDC poll interval.'

const MAX_LAG_SAMPLES = 10_000

async function main() {
  const config = loadConfig()
  const systemInfo = collectSystemInfo()
  const driver = await loadBenchDriver()
  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cdc-bench-'))
  const dbPath = join(tempDir, 'cdc-bench.db')

  const db = await Database.create('cdc-bench', dbPath, driver, {
    cdcPollInterval: 1,
    walMode: true,
  })

  try {
    await db.execute('CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, insert_ts REAL, data TEXT)')
    await db.watch('events')

    const keepAlive = setInterval(() => {}, 60_000)

    let writeCount = 0
    let cdcCount = 0
    const lagSamples: number[] = []

    const subscription = db.on('events').subscribe(event => {
      cdcCount++
      const row = event.row as { insert_ts: number } | undefined
      if (row?.insert_ts) {
        const lag = performance.now() - row.insert_ts
        if (lagSamples.length < MAX_LAG_SAMPLES) {
          lagSamples.push(lag)
        } else {
          const idx = Math.floor(Math.random() * cdcCount)
          if (idx < MAX_LAG_SAMPLES) lagSamples[idx] = lag
        }
      }
    })

    const duration = config.measureTime
    const deadline = performance.now() + duration

    while (performance.now() < deadline) {
      const ts = performance.now()
      await db.execute('INSERT INTO events (insert_ts, data) VALUES (?, ?)', [ts, `payload-${writeCount}`])
      writeCount++
    }

    const drainDeadline = Date.now() + 3_000
    while (cdcCount < writeCount && Date.now() < drainDeadline) {
      await new Promise(r => setTimeout(r, 10))
    }

    subscription.unsubscribe()
    clearInterval(keepAlive)

    const actualDurationSec = duration / 1000
    const cdcEventsPerSec = cdcCount / actualDurationSec
    const writeOpsPerSec = writeCount / actualDurationSec
    const deliveryRatio = writeCount > 0 ? cdcCount / writeCount : 0

    lagSamples.sort((a, b) => a - b)
    const meanLag = lagSamples.length > 0 ? lagSamples.reduce((sum, v) => sum + v, 0) / lagSamples.length : 0
    const p50Lag = lagSamples[Math.floor(lagSamples.length * 0.5)] ?? 0
    const p99Lag = lagSamples[Math.floor(lagSamples.length * 0.99)] ?? 0
    const p999Lag = lagSamples[Math.floor(lagSamples.length * 0.999)] ?? 0
    const minLag = lagSamples[0] ?? 0
    const maxLag = lagSamples[lagSamples.length - 1] ?? 0
    const variance =
      lagSamples.length > 0 ? lagSamples.reduce((sum, v) => sum + (v - meanLag) ** 2, 0) / lagSamples.length : 0
    const sdLag = Math.sqrt(variance)
    const cvLag = meanLag > 0 ? sdLag / meanLag : 0

    console.log('\n=== CDC Throughput Benchmark ===')
    console.log(`Duration:        ${actualDurationSec.toFixed(1)}s`)
    console.log(`Writes:          ${writeCount.toLocaleString()}`)
    console.log(`CDC events:      ${cdcCount.toLocaleString()}`)
    console.log(`Delivery ratio:  ${(deliveryRatio * 100).toFixed(1)}%`)
    console.log(`Write rate:      ${writeOpsPerSec.toFixed(0)} ops/s`)
    console.log(`CDC rate:        ${cdcEventsPerSec.toFixed(0)} events/s`)
    console.log(`Lag mean:        ${meanLag.toFixed(3)} ms`)
    console.log(`Lag P50:         ${p50Lag.toFixed(3)} ms`)
    console.log(`Lag P99:         ${p99Lag.toFixed(3)} ms`)
    console.log(`Lag max:         ${maxLag.toFixed(3)} ms`)
    console.log(`Lag SD:          ${sdLag.toFixed(3)} ms`)
    console.log(`Lag CV:          ${(cvLag * 100).toFixed(1)}%${cvLag > 0.1 ? ' [!]' : ''}`)
    console.log('================================\n')

    const msToNs = 1_000_000

    writeSirannonOnlyResults('cdc-latency', systemInfo, [
      {
        workload: 'cdc-throughput',
        framing: FRAMING,
        result: {
          name: 'cdc-throughput',
          opsPerSec: cdcEventsPerSec,
          meanNs: meanLag * msToNs,
          p50Ns: p50Lag * msToNs,
          p75Ns: (lagSamples[Math.floor(lagSamples.length * 0.75)] ?? 0) * msToNs,
          p99Ns: p99Lag * msToNs,
          p999Ns: p999Lag * msToNs,
          minNs: minLag * msToNs,
          maxNs: maxLag * msToNs,
          sdNs: sdLag * msToNs,
          cv: cvLag,
          moe: 0,
          samples: lagSamples.length,
        },
      },
      {
        workload: 'cdc-write-rate',
        framing: FRAMING,
        result: {
          name: 'cdc-write-rate',
          opsPerSec: writeOpsPerSec,
          meanNs: writeOpsPerSec > 0 ? (1 / writeOpsPerSec) * 1e9 : 0,
          p50Ns: 0,
          p75Ns: 0,
          p99Ns: 0,
          p999Ns: 0,
          minNs: 0,
          maxNs: 0,
          sdNs: 0,
          cv: 0,
          moe: 0,
          samples: writeCount,
        },
      },
    ])
  } finally {
    if (!db.closed) {
      await db.close()
    }
    rmSync(tempDir, { recursive: true, force: true })
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
