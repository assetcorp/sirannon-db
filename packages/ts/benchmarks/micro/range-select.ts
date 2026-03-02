import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Range scan returning ~10 rows per query via BETWEEN on indexed age column. ' +
  'Tests index scan and result serialization. ' +
  'Does not test full table scans or large result set streaming.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping range-select benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const dataSize of config.dataSizes) {
    const sirannonEngine = createSirannonEngine(config)
    const postgresEngine = createPostgresEngine(config)

    const sqliteSchemaWithIndex = `${microSchemaSqlite};\nCREATE INDEX IF NOT EXISTS idx_users_age ON users(age)`
    const pgSchemaWithIndex = `${microSchemaPostgres};\nCREATE INDEX IF NOT EXISTS idx_users_age ON users(age)`

    await sirannonEngine.setup(sqliteSchemaWithIndex)
    await postgresEngine.setup(pgSchemaWithIndex)

    const rows = Array.from({ length: dataSize }, (_, i) => generateUserRow(i + 1))
    await sirannonEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
    await postgresEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5)', rows)

    const zipfian = new ZipfianGenerator(70)
    const ageStarts = Array.from({ length: 10_000 }, () => 18 + zipfian.next())
    let sirannonIdx = 0
    let postgresIdx = 0

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    await pool.query({
      name: `rs-warmup-${dataSize}`,
      text: 'SELECT * FROM users WHERE age BETWEEN $1 AND $2',
      values: [25, 35],
    })

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) {
      systemInfo.postgresVersion = pgInfo.version ?? ''
    }

    pairs.push({
      workload: 'range-select',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `range-select [${dataSize}]`,
        fn: () => {
          const start = ageStarts[sirannonIdx++ % ageStarts.length]
          db.query('SELECT * FROM users WHERE age BETWEEN ? AND ?', [start, start + 10])
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `range-select [${dataSize}]`,
        fn: async () => {
          const start = ageStarts[postgresIdx++ % ageStarts.length]
          await pool.query({
            name: `rs-${dataSize}`,
            text: 'SELECT * FROM users WHERE age BETWEEN $1 AND $2',
            values: [start, start + 10],
          })
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'micro-range-select', ...config }, pairs)
  writeResults('micro-range-select', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
