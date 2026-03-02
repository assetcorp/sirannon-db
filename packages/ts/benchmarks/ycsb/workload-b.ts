import { randomBytes } from 'node:crypto'
import { collectSystemInfo, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { type ComparisonPair, runComparison } from '../runner'
import { generateYcsbRow, ycsbSchema, ZipfianGenerator } from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const READ_RATIO = 0.95
const FRAMING =
  'YCSB Workload B: 95% read, 5% update with Zipfian key distribution (theta=0.99). ' +
  'Represents read-heavy workloads like user session stores and tag/comment lookups. ' +
  'Does not test concurrent multi-client access patterns.'

function randomField(): string {
  return randomBytes(50).toString('hex')
}

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping YCSB Workload B.')
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
      name: `ycsbb-read-warmup-${dataSize}`,
      text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
      values: ['user0'],
    })
    await pool.query({
      name: `ycsbb-update-warmup-${dataSize}`,
      text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
      values: ['warmup', 'user0'],
    })

    pairs.push({
      workload: 'ycsb-b',
      dataSize,
      framing: FRAMING,
      sirannon: {
        name: `ycsb-b [${dataSize}]`,
        fn: () => {
          const key = keys[sirannonIdx++ % keys.length]
          if (Math.random() < READ_RATIO) {
            db.query('SELECT * FROM usertable WHERE ycsb_key = ?', [key])
          } else {
            db.execute('UPDATE usertable SET field0 = ? WHERE ycsb_key = ?', [randomField(), key])
          }
        },
        opts: { async: false },
        afterAll: async () => {
          await sirannonEngine.cleanup()
        },
      },
      postgres: {
        name: `ycsb-b [${dataSize}]`,
        fn: async () => {
          const key = keys[postgresIdx++ % keys.length]
          if (Math.random() < READ_RATIO) {
            await pool.query({
              name: `ycsbb-read-${dataSize}`,
              text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
              values: [key],
            })
          } else {
            await pool.query({
              name: `ycsbb-update-${dataSize}`,
              text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
              values: [randomField(), key],
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

  const results = await runComparison({ category: 'ycsb-b', ...config }, pairs)
  writeResults('ycsb-b', systemInfo, results)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
