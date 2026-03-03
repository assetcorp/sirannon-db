import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { getGlobalRng, resetGlobalRng } from '../rng'
import type { ComparisonPair } from '../runner'
import { runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const READ_RATIO = 0.8
const FRAMING =
  'Batch throughput of N mixed queries (80% read, 20% write) at varying batch sizes. ' +
  'Sirannon executes queries sequentially with zero network overhead (embedded, ' +
  'event-loop blocking). Postgres dispatches queries concurrently across pool ' +
  'connections (network I/O overlaps). SQLite serializes writes; Postgres uses MVCC. ' +
  'For the full concurrent HTTP story under realistic load, see the k6 e2e Docker benchmarks.'

const CONCURRENCY_LEVELS = [1, 4, 8, 16]
const DATA_SIZE = 10_000

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping concurrent mixed benchmark.')
    process.exit(0)
  }

  resetGlobalRng()
  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const concurrency of CONCURRENCY_LEVELS) {
    const sirannonEngine = createSirannonEngine(config, { readPoolSize: concurrency })
    const postgresEngine = createPostgresEngine({
      ...config,
      postgres: { ...config.postgres, max: concurrency },
    })

    await sirannonEngine.setup(microSchemaSqlite)
    await postgresEngine.setup(microSchemaPostgres)

    const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
    await sirannonEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
    await postgresEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5)', rows)

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) systemInfo.postgresVersion = pgInfo.version ?? ''

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    const zipfian = new ZipfianGenerator(DATA_SIZE)
    const ids = Array.from({ length: 10_000 }, () => (zipfian.next() % DATA_SIZE) + 1)

    await pool.query({
      name: `concurrent-mixed-read-warmup-${concurrency}`,
      text: 'SELECT * FROM users WHERE id = $1',
      values: [1],
    })
    await pool.query({
      name: `concurrent-mixed-write-warmup-${concurrency}`,
      text: 'UPDATE users SET age = $1 WHERE id = $2',
      values: [25, 1],
    })

    let sirannonIdx = 0
    let postgresIdx = 0

    pairs.push({
      workload: `concurrent-mixed-c${concurrency}`,
      dataSize: DATA_SIZE,
      framing: FRAMING,
      sirannon: {
        name: `concurrent-mixed [c${concurrency}]`,
        fn: () => {
          for (let c = 0; c < concurrency; c++) {
            const id = ids[sirannonIdx++ % ids.length]
            const isRead = getGlobalRng().next() < READ_RATIO
            if (isRead) {
              db.query('SELECT * FROM users WHERE id = ?', [id])
            } else {
              const age = getGlobalRng().nextInt(62) + 18
              db.execute('UPDATE users SET age = ? WHERE id = ?', [age, id])
            }
          }
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `concurrent-mixed [c${concurrency}]`,
        fn: async () => {
          const tasks: Promise<void>[] = []
          for (let c = 0; c < concurrency; c++) {
            const id = ids[postgresIdx++ % ids.length]
            const isRead = getGlobalRng().next() < READ_RATIO
            if (isRead) {
              tasks.push(
                pool
                  .query({
                    name: `concurrent-mixed-read-${concurrency}`,
                    text: 'SELECT * FROM users WHERE id = $1',
                    values: [id],
                  })
                  .then(() => {}),
              )
            } else {
              const age = getGlobalRng().nextInt(62) + 18
              tasks.push(
                (async () => {
                  const client = await pool.connect()
                  try {
                    await client.query('BEGIN')
                    await client.query({
                      name: `concurrent-mixed-write-${concurrency}`,
                      text: 'UPDATE users SET age = $1 WHERE id = $2',
                      values: [age, id],
                    })
                    await client.query('COMMIT')
                  } catch (err) {
                    await client.query('ROLLBACK')
                    throw err
                  } finally {
                    client.release()
                  }
                })(),
              )
            }
          }
          await Promise.all(tasks)
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'concurrent-mixed', ...config }, pairs)
  writeResults('concurrent-mixed', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
