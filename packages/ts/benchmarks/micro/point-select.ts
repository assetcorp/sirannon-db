import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateUserRow, microSchemaPostgres, microSchemaSqlite, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'Single-client point query by primary key. Measures query engine overhead and IPC cost. ' +
  'Postgres incurs TCP serialization; SQLite runs in-process. ' +
  'Does not test concurrent multi-client throughput.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping point-select benchmark.')
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

    const zipfian = new ZipfianGenerator(dataSize)
    const queryIds = Array.from({ length: 10_000 }, () => (zipfian.next() % dataSize) + 1)
    let sirannonIdx = 0
    let postgresIdx = 0

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) {
      systemInfo.postgresVersion = pgInfo.version ?? ''
    }
    const sInfo = await sirannonEngine.getInfo()
    if (!systemInfo.sqliteVersion) {
      systemInfo.sqliteVersion = sInfo.page_size ? `PRAGMAs: ${JSON.stringify(sInfo)}` : ''
    }

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    await pool.query({ name: `ps-warmup-${dataSize}`, text: 'SELECT * FROM users WHERE id = $1', values: [1] })

    pairs.push({
      workload: 'point-select',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `point-select [${dataSize}]`,
        fn: () => {
          const id = queryIds[sirannonIdx++ % queryIds.length]
          db.query('SELECT * FROM users WHERE id = ?', [id])
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `point-select [${dataSize}]`,
        fn: async () => {
          const id = queryIds[postgresIdx++ % queryIds.length]
          await pool.query({ name: `ps-${dataSize}`, text: 'SELECT * FROM users WHERE id = $1', values: [id] })
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'micro-point-select', ...config }, pairs)
  writeResults('micro-point-select', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
