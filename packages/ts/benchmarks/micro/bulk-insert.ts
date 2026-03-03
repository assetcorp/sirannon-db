import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Per-row insert throughput inside a single transaction. Both engines use individual ' +
  'prepared-statement INSERTs (no batching or multi-value tricks). Tests write path overhead, ' +
  'WAL performance, and transaction commit cost. Does not test concurrent writers.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping bulk-insert benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const bulkSize of config.dataSizes) {
    const rows = Array.from({ length: bulkSize }, (_, i) => generateUserRow(i + 1))

    const sirannonEngine = createSirannonEngine(config)
    const postgresEngine = createPostgresEngine(config)

    await sirannonEngine.setup(microSchemaSqlite)
    await postgresEngine.setup(microSchemaPostgres)

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) {
      systemInfo.postgresVersion = pgInfo.version ?? ''
    }

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    pairs.push({
      workload: `bulk-insert-${bulkSize}`,
      dataSize: bulkSize,
      framing: FRAMING,
      sirannon: {
        name: `bulk-insert [${bulkSize}]`,
        fn: () => {
          db.transaction(tx => {
            tx.execute('DELETE FROM users')
            for (const row of rows) {
              tx.execute('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', row)
            }
          })
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `bulk-insert [${bulkSize}]`,
        fn: async () => {
          const client = await pool.connect()
          try {
            await client.query('BEGIN')
            await client.query('DELETE FROM users')
            for (const row of rows) {
              await client.query({
                name: `bulk-insert-row-${bulkSize}`,
                text: 'INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5)',
                values: row,
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

  const results = await runComparison({ category: 'micro-bulk-insert', ...config }, pairs)
  writeResults('micro-bulk-insert', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
