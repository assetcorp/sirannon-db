import { getGlobalRng } from '../rng'
import {
  generateCustomer,
  generateProduct,
  generateUserRow,
  generateYcsbRow,
  microSchemaPostgres,
  microSchemaSqlite,
  tpccSchemaPostgres,
  tpccSchemaSqlite,
  ycsbSchema,
  ZipfianGenerator,
} from '../schemas'

export interface WorkloadConfig {
  name: string
  dataSize: number
  warmupMs: number
  measureMs: number
}

export interface WorkloadDefinition {
  name: string
  category: string
  sqliteSchema: string
  postgresSchema: string
  seedFn: (dataSize: number) => { insertSql: string; rows: unknown[][] }[]
  operations: WorkloadOperation[]
}

/**
 * A workload operation. Reads run `sqliteSql`/`postgresSql`; writes run the same fields as a
 * statement with no result set. A read-modify-write runs the read followed by the write in the
 * `writeSqliteSql`/`writePostgresSql` fields against the same key: for `rmw`, `paramsFn` returns
 * `[key, newValue]`, the read binds `[key]`, and the write binds `[newValue, key]`.
 */
export interface WorkloadOperation {
  name: string
  weight: number
  kind: 'read' | 'write' | 'rmw'
  sqliteSql: string
  postgresSql: string
  writeSqliteSql?: string
  writePostgresSql?: string
  paramsFn: (dataSize: number) => unknown[]
}

function userSeed(dataSize: number) {
  return [
    {
      insertSql: 'INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)',
      rows: Array.from({ length: dataSize }, (_, i) => generateUserRow(i + 1)),
    },
  ]
}

function ycsbSeed(dataSize: number) {
  return [
    {
      insertSql:
        'INSERT INTO usertable (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      rows: Array.from({ length: dataSize }, (_, i) => generateYcsbRow(`user${i}`)),
    },
  ]
}

function tpccSeed(dataSize: number) {
  const customerCount = dataSize
  const productCount = Math.min(dataSize, 1000)
  return [
    {
      insertSql: 'INSERT INTO customers (id, name, email, balance) VALUES (?, ?, ?, ?)',
      rows: Array.from({ length: customerCount }, (_, i) => generateCustomer(i + 1)),
    },
    {
      insertSql: 'INSERT INTO products (id, name, price, stock) VALUES (?, ?, ?, ?)',
      rows: Array.from({ length: productCount }, (_, i) => generateProduct(i + 1)),
    },
  ]
}

const zipfCache = new Map<number, ZipfianGenerator>()
function getZipf(dataSize: number): ZipfianGenerator {
  let gen = zipfCache.get(dataSize)
  if (!gen) {
    gen = new ZipfianGenerator(dataSize)
    zipfCache.set(dataSize, gen)
  }
  return gen
}

const YCSB_READ_SQLITE = 'SELECT * FROM usertable WHERE ycsb_key = ?'
const YCSB_READ_POSTGRES = 'SELECT * FROM usertable WHERE ycsb_key = $1'
const YCSB_UPDATE_SQLITE = 'UPDATE usertable SET field0 = ? WHERE ycsb_key = ?'
const YCSB_UPDATE_POSTGRES = 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2'

function ycsbKey(dataSize: number): string {
  return `user${getZipf(dataSize).next()}`
}

function ycsbValue(): string {
  return `value_${getGlobalRng().nextInt(1_000_000)}`
}

function ycsbRead(weight: number): WorkloadOperation {
  return {
    name: 'read',
    weight,
    kind: 'read',
    sqliteSql: YCSB_READ_SQLITE,
    postgresSql: YCSB_READ_POSTGRES,
    paramsFn: dataSize => [ycsbKey(dataSize)],
  }
}

function ycsbUpdate(weight: number): WorkloadOperation {
  return {
    name: 'update',
    weight,
    kind: 'write',
    sqliteSql: YCSB_UPDATE_SQLITE,
    postgresSql: YCSB_UPDATE_POSTGRES,
    paramsFn: dataSize => [ycsbValue(), ycsbKey(dataSize)],
  }
}

function ycsbReadModifyWrite(weight: number): WorkloadOperation {
  return {
    name: 'read-modify-write',
    weight,
    kind: 'rmw',
    sqliteSql: YCSB_READ_SQLITE,
    postgresSql: YCSB_READ_POSTGRES,
    writeSqliteSql: YCSB_UPDATE_SQLITE,
    writePostgresSql: YCSB_UPDATE_POSTGRES,
    paramsFn: dataSize => [ycsbKey(dataSize), ycsbValue()],
  }
}

function ycsbWorkload(name: string, operations: WorkloadOperation[]): WorkloadDefinition {
  return { name, category: 'ycsb', sqliteSchema: ycsbSchema, postgresSchema: ycsbSchema, seedFn: ycsbSeed, operations }
}

/**
 * The YCSB core workloads that map cleanly onto a fixed-size, string-keyed table. A (update-heavy),
 * B (read-mostly), C (read-only), and F (read-modify-write) all run against the same seeded rows.
 * D (read-latest) and E (short-range-scan) are omitted: D needs insert-driven key growth during the
 * run and E needs ordered-key range scans, and per the YCSB recipe E runs on a separately loaded
 * database. Running a stated subset is the documented, accepted practice.
 */
function ycsbWorkloads(): WorkloadDefinition[] {
  return [
    ycsbWorkload('ycsb-a', [ycsbRead(0.5), ycsbUpdate(0.5)]),
    ycsbWorkload('ycsb-b', [ycsbRead(0.95), ycsbUpdate(0.05)]),
    ycsbWorkload('ycsb-c', [ycsbRead(1)]),
    ycsbWorkload('ycsb-f', [ycsbRead(0.5), ycsbReadModifyWrite(0.5)]),
  ]
}

export const workloads: WorkloadDefinition[] = [
  {
    name: 'point-select',
    category: 'micro',
    sqliteSchema: microSchemaSqlite,
    postgresSchema: microSchemaPostgres,
    seedFn: userSeed,
    operations: [
      {
        name: 'point-select',
        weight: 1,
        kind: 'read',
        sqliteSql: 'SELECT * FROM users WHERE id = ?',
        postgresSql: 'SELECT * FROM users WHERE id = $1',
        paramsFn: dataSize => [getZipf(dataSize).next() + 1],
      },
    ],
  },
  {
    name: 'bulk-insert',
    category: 'micro',
    sqliteSchema: microSchemaSqlite,
    postgresSchema: microSchemaPostgres,
    seedFn: () => [],
    operations: [
      {
        name: 'bulk-insert',
        weight: 1,
        kind: 'write',
        sqliteSql: 'INSERT OR REPLACE INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)',
        postgresSql:
          'INSERT INTO users (id, name, email, age, bio) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        paramsFn: dataSize => {
          const id = Math.floor(getGlobalRng().next() * dataSize) + 1
          return generateUserRow(id)
        },
      },
    ],
  },
  {
    name: 'batch-update',
    category: 'micro',
    sqliteSchema: microSchemaSqlite,
    postgresSchema: microSchemaPostgres,
    seedFn: userSeed,
    operations: [
      {
        name: 'batch-update',
        weight: 1,
        kind: 'write',
        sqliteSql: 'UPDATE users SET age = ? WHERE id = ?',
        postgresSql: 'UPDATE users SET age = $1 WHERE id = $2',
        paramsFn: dataSize => {
          const id = getZipf(dataSize).next() + 1
          return [Math.floor(getGlobalRng().next() * 80) + 18, id]
        },
      },
    ],
  },
  ...ycsbWorkloads(),
  {
    name: 'tpc-c-derived',
    category: 'oltp',
    sqliteSchema: tpccSchemaSqlite,
    postgresSchema: tpccSchemaPostgres,
    seedFn: tpccSeed,
    operations: [
      {
        name: 'new-order',
        weight: 0.5,
        kind: 'write',
        sqliteSql: 'INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)',
        postgresSql: 'INSERT INTO orders (customer_id, total, status, created_at) VALUES ($1, $2, $3, $4)',
        paramsFn: dataSize => {
          const customerId = Math.floor(getGlobalRng().next() * dataSize) + 1
          const total = Math.round(getGlobalRng().next() * 500 * 100) / 100
          return [customerId, total, 'pending', new Date().toISOString()]
        },
      },
      {
        name: 'payment',
        weight: 0.5,
        kind: 'write',
        sqliteSql: 'UPDATE customers SET balance = balance - ? WHERE id = ?',
        postgresSql: 'UPDATE customers SET balance = balance - $1 WHERE id = $2',
        paramsFn: dataSize => {
          const amount = Math.round(getGlobalRng().next() * 100 * 100) / 100
          const customerId = Math.floor(getGlobalRng().next() * dataSize) + 1
          return [amount, customerId]
        },
      },
    ],
  },
]

export function getWorkload(name: string): WorkloadDefinition | undefined {
  return workloads.find(w => w.name === name)
}

export function pickOperation(ops: WorkloadOperation[]): WorkloadOperation {
  const r = getGlobalRng().next()
  let cumulative = 0
  for (const op of ops) {
    cumulative += op.weight
    if (r < cumulative) return op
  }
  return ops[ops.length - 1]
}
