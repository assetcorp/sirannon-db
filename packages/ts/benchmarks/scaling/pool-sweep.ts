import { collectSystemInfo, loadBenchDriver, loadConfig } from '../config'
import { createPostgresEngine, isPostgresAvailable } from '../postgres-engine'
import { writeResults } from '../reporter'
import { getGlobalRng } from '../rng'
import { type ComparisonPair, runComparison } from '../runner'
import {
  generateUserRow,
  generateYcsbRow,
  microSchemaPostgres,
  microSchemaSqlite,
  ycsbSchema,
  ZipfianGenerator,
} from '../schemas'
import { createSirannonEngine } from '../sirannon-engine'

const POOL_SIZES = [5, 10, 20]
const MAX_DATA_SIZE = 100_000
const READ_RATIO = 0.5

const FRAMING =
  'Pool size sweep: measures how Postgres connection pool size (5, 10, 20) ' +
  'affects throughput relative to Sirannon. Sirannon uses a fixed read pool of 4. ' +
  'Tests point-select (read-heavy) and YCSB-A (mixed 50/50) workloads.'

async function main() {
  const config = loadConfig()
  const pgAvailable = await isPostgresAvailable(config)

  if (!pgAvailable) {
    console.log('Postgres not available, skipping pool-sweep benchmark.')
    process.exit(0)
  }

  const systemInfo = collectSystemInfo()
  const driver = await loadBenchDriver()
  const dataSizes = config.dataSizes.filter(s => s <= MAX_DATA_SIZE)

  if (dataSizes.length < config.dataSizes.length) {
    const skipped = config.dataSizes.filter(s => s > MAX_DATA_SIZE)
    console.log(
      `Pool-sweep capped at ${MAX_DATA_SIZE.toLocaleString()} rows. Skipping: ${skipped.map(s => s.toLocaleString()).join(', ')}`,
    )
  }

  const allResults: Awaited<ReturnType<typeof runComparison>> = []

  for (const poolSize of POOL_SIZES) {
    console.log(`\n=== Pool size: ${poolSize} ===`)
    const pgConfig = { ...config, postgres: { ...config.postgres, max: poolSize } }
    const pairs: ComparisonPair[] = []
    const engines: Array<{ cleanup: () => Promise<void> }> = []

    try {
      for (const dataSize of dataSizes) {
        const sirannonEngine = createSirannonEngine(driver, config)
        engines.push(sirannonEngine)
        const postgresEngine = createPostgresEngine(pgConfig)
        engines.push(postgresEngine)

        await sirannonEngine.setup(microSchemaSqlite)
        await postgresEngine.setup(microSchemaPostgres)

        const userRows = Array.from({ length: dataSize }, (_, i) => generateUserRow(i + 1))
        await sirannonEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', userRows)
        await postgresEngine.seed('INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5)', userRows)

        const pgInfo = await postgresEngine.getInfo()
        if (!systemInfo.postgresVersion) systemInfo.postgresVersion = pgInfo.version ?? ''

        const db = sirannonEngine.db
        const pool = postgresEngine.pool

        const zipfian = new ZipfianGenerator(dataSize)
        const queryIds = Array.from({ length: 10_000 }, () => (zipfian.next() % dataSize) + 1)
        let sirannonPsIdx = 0
        let postgresPsIdx = 0

        await pool.query({
          name: `ps-warmup-${poolSize}-${dataSize}`,
          text: 'SELECT * FROM users WHERE id = $1',
          values: [1],
        })

        pairs.push({
          workload: `point-select (pool=${poolSize})`,
          dataSize,
          framing: FRAMING,
          sirannon: {
            name: `point-select (pool=${poolSize}) [${dataSize}]`,
            fn: async () => {
              const id = queryIds[sirannonPsIdx++ % queryIds.length]
              await db.query('SELECT * FROM users WHERE id = ?', [id])
            },
            opts: { async: true },
            afterAll: async () => {
              await sirannonEngine.cleanup()
            },
          },
          postgres: {
            name: `point-select (pool=${poolSize}) [${dataSize}]`,
            fn: async () => {
              const id = queryIds[postgresPsIdx++ % queryIds.length]
              await pool.query({
                name: `ps-${poolSize}-${dataSize}`,
                text: 'SELECT * FROM users WHERE id = $1',
                values: [id],
              })
            },
            opts: { async: true },
            afterAll: async () => {
              await postgresEngine.cleanup()
            },
          },
        })

        const sirannonYcsbEngine = createSirannonEngine(driver, config)
        engines.push(sirannonYcsbEngine)
        const postgresYcsbEngine = createPostgresEngine(pgConfig)
        engines.push(postgresYcsbEngine)

        await sirannonYcsbEngine.setup(ycsbSchema)
        await postgresYcsbEngine.setup(ycsbSchema)

        const ycsbRows = Array.from({ length: dataSize }, (_, i) => generateYcsbRow(`user${i}`))
        await sirannonYcsbEngine.seed(
          'INSERT INTO usertable (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
          ycsbRows,
        )
        await postgresYcsbEngine.seed(
          'INSERT INTO usertable (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
          ycsbRows,
        )

        const ycsbZipfian = new ZipfianGenerator(dataSize)
        const keys = Array.from({ length: 10_000 }, () => `user${ycsbZipfian.next() % dataSize}`)
        const ops = Array.from({ length: 10_000 }, () => ({
          isRead: getGlobalRng().next() < READ_RATIO,
          field: Array.from(getGlobalRng().nextBytes(50), b => b.toString(16).padStart(2, '0')).join(''),
        }))
        let sirannonYcsbIdx = 0
        let postgresYcsbIdx = 0
        let sirannonOpIdx = 0
        let postgresOpIdx = 0

        const ycsbDb = sirannonYcsbEngine.db
        const ycsbPool = postgresYcsbEngine.pool

        await ycsbPool.query({
          name: `ycsba-read-warmup-${poolSize}-${dataSize}`,
          text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
          values: ['user0'],
        })
        await ycsbPool.query({
          name: `ycsba-update-warmup-${poolSize}-${dataSize}`,
          text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
          values: ['warmup', 'user0'],
        })

        pairs.push({
          workload: `ycsb-a (pool=${poolSize})`,
          dataSize,
          framing: FRAMING,
          sirannon: {
            name: `ycsb-a (pool=${poolSize}) [${dataSize}]`,
            fn: async () => {
              const key = keys[sirannonYcsbIdx++ % keys.length]
              const op = ops[sirannonOpIdx++ % ops.length]
              if (op.isRead) {
                await ycsbDb.query('SELECT * FROM usertable WHERE ycsb_key = ?', [key])
              } else {
                await ycsbDb.execute('UPDATE usertable SET field0 = ? WHERE ycsb_key = ?', [op.field, key])
              }
            },
            opts: { async: true },
            afterAll: async () => {
              await sirannonYcsbEngine.cleanup()
            },
          },
          postgres: {
            name: `ycsb-a (pool=${poolSize}) [${dataSize}]`,
            fn: async () => {
              const key = keys[postgresYcsbIdx++ % keys.length]
              const op = ops[postgresOpIdx++ % ops.length]
              if (op.isRead) {
                await ycsbPool.query({
                  name: `ycsba-read-${poolSize}-${dataSize}`,
                  text: 'SELECT * FROM usertable WHERE ycsb_key = $1',
                  values: [key],
                })
              } else {
                await ycsbPool.query({
                  name: `ycsba-update-${poolSize}-${dataSize}`,
                  text: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
                  values: [op.field, key],
                })
              }
            },
            opts: { async: true },
            afterAll: async () => {
              await postgresYcsbEngine.cleanup()
            },
          },
        })
      }

      const results = await runComparison({ category: `pool-sweep-${poolSize}`, ...config }, pairs)
      allResults.push(...results)
    } catch (err) {
      console.error(`Pool size ${poolSize} failed:`, err)
      for (const engine of engines) {
        try {
          await engine.cleanup()
        } catch {
          // engine may already be cleaned up
        }
      }
    }
  }

  writeResults('pool-sweep', systemInfo, allResults)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
