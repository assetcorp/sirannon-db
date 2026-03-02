import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Transactional batch update of 100 random rows per iteration. ' +
  'Tests write lock contention and transaction overhead. ' +
  'Does not test concurrent multi-writer scenarios where Postgres has structural advantages.'

const BATCH_SIZE = 100

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping batch-update benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const dataSize of config.dataSizes) {
    const sirannonEngine = createSirannonEngine(config)
    const postgresEngine = createPostgresEngine(config)

    await sirannonEngine.setup(microSchemaSqlite)
    await postgresEngine.setup(microSchemaPostgres)

    const rows = Array.from({ length: dataSize }, (_, i) => generateUserRow(i + 1))
    await sirannonEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
    await postgresEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5)', rows)

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) {
      systemInfo.postgresVersion = pgInfo.version ?? ''
    }

    const zipfian = new ZipfianGenerator(dataSize)
    const updateBatches: number[][] = []
    for (let b = 0; b < 10_000; b++) {
      const batch: number[] = []
      for (let i = 0; i < BATCH_SIZE; i++) {
        batch.push((zipfian.next() % dataSize) + 1)
      }
      updateBatches.push(batch)
    }

    let sirannonIdx = 0
    let postgresIdx = 0

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    await pool.query({
      name: `bu-warmup-${dataSize}`,
      text: 'UPDATE users SET age = $1 WHERE id = $2',
      values: [25, 1],
    })

    pairs.push({
      workload: 'batch-update',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `batch-update [${dataSize}]`,
        fn: () => {
          const batch = updateBatches[sirannonIdx++ % updateBatches.length]
          db.transaction(tx => {
            for (const id of batch) {
              tx.execute('UPDATE users SET age = ? WHERE id = ?', [20 + (id % 60), id])
            }
          })
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `batch-update [${dataSize}]`,
        fn: async () => {
          const batch = updateBatches[postgresIdx++ % updateBatches.length]
          const client = await pool.connect()
          try {
            await client.query('BEGIN')
            for (const id of batch) {
              await client.query({
                name: `bu-${dataSize}`,
                text: 'UPDATE users SET age = $1 WHERE id = $2',
                values: [20 + (id % 60), id],
              })
            }
            await client.query('COMMIT')
          } catch (err) {
            await client.query('ROLLBACK')
            throw err
          } finally {
            client.release()
          }
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'micro-batch-update', ...config }, pairs)
  writeResults('micro-batch-update', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
