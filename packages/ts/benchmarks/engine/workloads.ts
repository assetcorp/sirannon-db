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

export interface WorkloadOperation {
  name: string
  weight: number
  sqliteSql: string
  postgresSql: string
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
        sqliteSql: 'UPDATE users SET age = ? WHERE id = ?',
        postgresSql: 'UPDATE users SET age = $1 WHERE id = $2',
        paramsFn: dataSize => {
          const id = getZipf(dataSize).next() + 1
          return [Math.floor(getGlobalRng().next() * 80) + 18, id]
        },
      },
    ],
  },
  {
    name: 'ycsb-a',
    category: 'ycsb',
    sqliteSchema: ycsbSchema,
    postgresSchema: ycsbSchema,
    seedFn: ycsbSeed,
    operations: [
      {
        name: 'read',
        weight: 0.5,
        sqliteSql: 'SELECT * FROM usertable WHERE ycsb_key = ?',
        postgresSql: 'SELECT * FROM usertable WHERE ycsb_key = $1',
        paramsFn: dataSize => [`user${getZipf(dataSize).next()}`],
      },
      {
        name: 'update',
        weight: 0.5,
        sqliteSql: 'UPDATE usertable SET field0 = ? WHERE ycsb_key = ?',
        postgresSql: 'UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2',
        paramsFn: dataSize => [`value_${getGlobalRng().nextInt(1_000_000)}`, `user${getZipf(dataSize).next()}`],
      },
    ],
  },
  {
    name: 'tpc-c-lite',
    category: 'oltp',
    sqliteSchema: tpccSchemaSqlite,
    postgresSchema: tpccSchemaPostgres,
    seedFn: tpccSeed,
    operations: [
      {
        name: 'new-order',
        weight: 0.5,
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
