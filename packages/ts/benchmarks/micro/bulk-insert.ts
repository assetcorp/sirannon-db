import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Bulk write throughput in a single transaction. Tests write path overhead, WAL performance, ' +
  'and transaction commit cost. Each iteration creates a fresh table and inserts all rows. ' +
  'Does not test concurrent writers or cross-transaction isolation.'

const BULK_SIZES = [1_000, 10_000]

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping bulk-insert benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const bulkSize of BULK_SIZES) {
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
            tx.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
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

            const colCount = 5
            const chunkSize = 500
            for (let offset = 0; offset < rows.length; offset += chunkSize) {
              const chunk = rows.slice(offset, offset + chunkSize)
              const values: unknown[] = []
              const placeholders: string[] = []

              for (let i = 0; i < chunk.length; i++) {
                const row = chunk[i]
                const rowPh: string[] = []
                for (let j = 0; j < colCount; j++) {
                  values.push(row[j])
                  rowPh.push(`$${i * colCount + j + 1}`)
                }
                placeholders.push(`(${rowPh.join(', ')})`)
              }

              await client.query(
                `INSERT INTO users (id, name, email, age, bio) VALUES ${placeholders.join(', ')}`,
                values,
              )
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
