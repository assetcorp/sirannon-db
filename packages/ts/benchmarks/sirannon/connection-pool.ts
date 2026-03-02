import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Database } from '../../src/core/database'
import { collectSystemInfo } from '../config'
import { type SirannonOnlyResult, writeSirannonOnlyResults } from '../reporter'
import { runBenchmark } from '../runner'
import { generateUserRow, microSchemaSqlite, ZipfianGenerator } from '../schemas'

const FRAMING =
  'Read throughput scaling with reader pool size under WAL mode. ' +
  'Measures connection pool round-robin efficiency. ' +
  'Each operation runs N sequential reads as one unit of work. ' +
  'Does not test concurrent writer contention.'

const DATA_SIZE = 10_000
const POOL_SIZES = [1, 2, 4, 8]

async function main() {
  const systemInfo = collectSystemInfo()
  const results: SirannonOnlyResult[] = []

  for (const poolSize of POOL_SIZES) {
    const tempDir = mkdtempSync(join(tmpdir(), `sirannon-pool-${poolSize}-`))
    const dbPath = join(tempDir, 'pool-bench.db')

    const db = new Database(`pool-bench-${poolSize}`, dbPath, {
      readPoolSize: poolSize,
      walMode: true,
    })

    for (const stmt of microSchemaSqlite
      .split(';')
      .map(s => s.trim())
      .filter(Boolean)) {
      db.execute(stmt)
    }

    const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
    db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)

    const zipfian = new ZipfianGenerator(DATA_SIZE)
    const queryIds = Array.from({ length: 10_000 }, () => (zipfian.next() % DATA_SIZE) + 1)
    let idx = 0

    const benchResults = await runBenchmark(
      { category: `connection-pool-${poolSize}`, warmupTime: 3_000, measureTime: 8_000 },
      [
        {
          name: `pool-size-${poolSize}`,
          fn: () => {
            for (let i = 0; i < poolSize; i++) {
              const id = queryIds[idx++ % queryIds.length]
              db.query('SELECT * FROM users WHERE id = ?', [id])
            }
          },
          opts: { async: false },
        },
      ],
    )

    db.close()
    rmSync(tempDir, { recursive: true, force: true })

    if (benchResults[0]) {
      results.push({
        workload: `pool-size-${poolSize} (${poolSize} reads/op)`,
        framing: FRAMING,
        result: benchResults[0],
      })
    }
  }

  console.log('\n=== Connection Pool Benchmark ===')
  for (const r of results) {
    console.log(
      `${r.workload}: ${r.result.opsPerSec.toFixed(0)} ops/s (P50: ${(r.result.p50Ns / 1_000).toFixed(2)} us)`,
    )
  }
  console.log('=================================\n')

  writeSirannonOnlyResults('connection-pool', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
