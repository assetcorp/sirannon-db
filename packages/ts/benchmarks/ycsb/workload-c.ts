import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateYcsbRow, ycsbSchema, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const FRAMING =
  'YCSB Workload C: 100% read with Zipfian key distribution (theta=0.99). ' +
  'Pure read throughput benchmark, representative of caching layers and read replicas. ' +
  'Does not test write performance or concurrent access.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping YCSB Workload C.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const pairs: ComparisonPair[] = []

  for (const dataSize of config.dataSizes) {
    const sirannonEngine = createSirannonEngine(config)
    const postgresEngine = createPostgresEngine(config)

    await sirannonEngine.setup(ycsbSchema)
    await postgresEngine.setup(ycsbSchema)

    const rows = Array.from({ length: dataSize }, (_, i) => generateYcsbRow(`user${i}`))
    await sirannonEngine.seed(
      'INSERT INTO usertable (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      rows,
    )
    await postgresEngine.seed(
      'INSERT INTO usertable (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
      rows,
    )

    const pgInfo = await postgresEngine.getInfo()
    if (!systemInfo.postgresVersion) systemInfo.postgresVersion = pgInfo.version ?? ''

    const zipfian = new ZipfianGenerator(dataSize)
    const keys = Array.from({ length: 10_000 }, () => `user${zipfian.next() % dataSize}`)
    let sirannonIdx = 0
    let postgresIdx = 0

    const db = sirannonEngine.db
    const pool = postgresEngine.pool

    await pool.query({
      name: `ycsbc-read-warmup-${dataSize}`,
      text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
      values: ['user0'],
    })

    pairs.push({
      workload: 'ycsb-c',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `ycsb-c [${dataSize}]`,
        fn: () => {
          const key = keys[sirannonIdx++ % keys.length]
          db.query('SELECT * FROM usertable WHERE ycsb_key = ?', [key])
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `ycsb-c [${dataSize}]`,
        fn: async () => {
          const key = keys[postgresIdx++ % keys.length]
          await pool.query({
            name: `ycsbc-read-${dataSize}`,
            text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
            values: [key],
          })
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'ycsb-c', ...config }, pairs)
  writeResults('ycsb-c', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
