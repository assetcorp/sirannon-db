import { collectSystemInfo, loadBenchDriver, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { getGlobalRng } from '../rng'
import { type ComparisonPair, runComparison } from '../runner'
import { generateYcsbRow, ycsbSchema, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const READ_RATIO = 0.5
const FRAMING =
  'YCSB Workload A: 50% read, 50% update with Zipfian key distribution (theta=0.99). ' +
  'Measures mixed read/write performance under realistic access skew. ' +
  'Does not test concurrent multi-client access patterns.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping YCSB Workload A.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const driver = await loadBenchDriver()
  const pairs: ComparisonPair[] = []

  for (const dataSize of config.dataSizes) {
    const sirannonEngine = createSirannonEngine(driver, config)
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
      name: `ycsba-read-warmup-${dataSize}`,
      text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
      values: ['user0'],
    })
    await pool.query({
      name: `ycsba-update-warmup-${dataSize}`,
      text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
      values: ['warmup', 'user0'],
    })

    const ops = Array.from({ length: 10_000 }, () => ({
      isRead: getGlobalRng().next() < READ_RATIO,
      field: Array.from(getGlobalRng().nextBytes(50), b => b.toString(16).padStart(2, '0')).join(''),
    }))
    let sirannonOpIdx = 0
    let postgresOpIdx = 0

    pairs.push({
      workload: 'ycsb-a',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `ycsb-a [${dataSize}]`,
        fn: async () => {
          const key = keys[sirannonIdx++ % keys.length]
          const op = ops[sirannonOpIdx++ % ops.length]
          if (op.isRead) {
            await db.query('SELECT * FROM usertable WHERE ycsb_key = ?', [key])
          } else {
            await db.execute('UPDATE usertable SET field0 = ? WHERE ycsb_key = ?', [op.field, key])
          }
        },
        opts: { async: true },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `ycsb-a [${dataSize}]`,
        fn: async () => {
          const key = keys[postgresIdx++ % keys.length]
          const op = ops[postgresOpIdx++ % ops.length]
          if (op.isRead) {
            await pool.query({
              name: `ycsba-read-${dataSize}`,
              text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
              values: [key],
            })
          } else {
            await pool.query({
              name: `ycsba-update-${dataSize}`,
              text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
              values: [op.field, key],
            })
          }
        },
        opts: { async: true },
        afterAll: async () => {
          await postgresEngine.cleanup()
        },
      },
    })
  }

  const results = await runComparison({ category: 'ycsb-a', ...config }, pairs)
  writeResults('ycsb-a', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
