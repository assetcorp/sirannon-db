// The workload catalogue both engines run identically. Each workload names its schema for each
// dialect, a seed that fills the table, and a weighted set of operations. Operations carry SQL for
// both dialects written with `?` placeholders; each driver renders the placeholder in its own
// style. Reads, writes, and read-modify-writes are drawn by weight, so a run reproduces the same
// operation stream given the same seed.
//
// The set is the standard OLTP core: point-select, bulk-insert, and batch-update as the atomic
// operations, YCSB A/B/C/F as the key-value mix, and a TPC-C-shaped transaction mix. YCSB D
// (read-latest) and E (short-range-scan) are left out because D needs insert-driven key growth
// during the run and E needs ordered-key range scans on a separately loaded table; running a
// stated subset is documented, accepted practice.

import type { SeededRng, ZipfianGenerator } from './rng.ts'

export interface OperationContext {
  rng: SeededRng
  zipf: ZipfianGenerator
  dataSize: number
}

export type OperationKind = 'read' | 'write' | 'rmw'

export interface Operation {
  name: string
  weight: number
  kind: OperationKind
  sqliteSql: string
  postgresSql: string
  params: (ctx: OperationContext) => unknown[]
  writeSqliteSql?: string
  writePostgresSql?: string
}

export interface SeedTable {
  table: string
  columns: string[]
  rows: Iterable<unknown[]>
}

export interface Workload {
  name: string
  category: string
  tables: string[]
  sqliteSchema: string
  postgresSchema: string
  seed: (rng: SeededRng, dataSize: number) => SeedTable[]
  operations: Operation[]
}

const YCSB_SCHEMA =
  'CREATE TABLE IF NOT EXISTS usertable (' +
  'ycsb_key TEXT PRIMARY KEY, ' +
  'field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, ' +
  'field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)'

const MICRO_SCHEMA =
  'CREATE TABLE IF NOT EXISTS users (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL, bio TEXT)'

const TPCC_SCHEMA_SQLITE =
  'CREATE TABLE IF NOT EXISTS customers (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, balance REAL NOT NULL);' +
  'CREATE TABLE IF NOT EXISTS products (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL);' +
  'CREATE TABLE IF NOT EXISTS orders (' +
  'id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, ' +
  'status TEXT NOT NULL, created_at TEXT NOT NULL)'

const TPCC_SCHEMA_POSTGRES =
  'CREATE TABLE IF NOT EXISTS customers (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, balance REAL NOT NULL);' +
  'CREATE TABLE IF NOT EXISTS products (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL);' +
  'CREATE TABLE IF NOT EXISTS orders (' +
  'id SERIAL PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, ' +
  'status TEXT NOT NULL, created_at TEXT NOT NULL)'

const FIRST_NAMES = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy']
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
const FIXED_TIMESTAMP = '2026-01-01T00:00:00.000Z'

function userRow(rng: SeededRng, rowId: number): unknown[] {
  const name = `${FIRST_NAMES[rowId % FIRST_NAMES.length]} ${LAST_NAMES[rowId % LAST_NAMES.length]}`
  const email = `user${rowId}@example.com`
  const age = 18 + (rowId % 62)
  const bio = `Bio for user ${rowId}: ${rng.text(50)}`
  return [rowId, name, email, age, bio]
}

function ycsbRow(rng: SeededRng, key: string): unknown[] {
  const fields: string[] = []
  for (let i = 0; i < 10; i++) {
    fields.push(rng.text(100))
  }
  return [key, ...fields]
}

function customerRow(rowId: number): unknown[] {
  return [rowId, `Customer ${rowId}`, `customer${rowId}@store.com`, 10000.0]
}

function productRow(rng: SeededRng, rowId: number): unknown[] {
  const price = Math.round((10 + rng.fraction() * 490) * 100) / 100
  return [rowId, `Product ${rowId}`, price, 1000]
}

function* userRows(rng: SeededRng, dataSize: number): Generator<unknown[]> {
  for (let i = 0; i < dataSize; i++) {
    yield userRow(rng, i + 1)
  }
}

function userSeed(rng: SeededRng, dataSize: number): SeedTable[] {
  return [{ table: 'users', columns: ['id', 'name', 'email', 'age', 'bio'], rows: userRows(rng, dataSize) }]
}

function emptySeed(): SeedTable[] {
  return []
}

function* ycsbRows(rng: SeededRng, dataSize: number): Generator<unknown[]> {
  for (let i = 0; i < dataSize; i++) {
    yield ycsbRow(rng, `user${i}`)
  }
}

function ycsbSeed(rng: SeededRng, dataSize: number): SeedTable[] {
  const columns = ['ycsb_key']
  for (let i = 0; i < 10; i++) {
    columns.push(`field${i}`)
  }
  return [{ table: 'usertable', columns, rows: ycsbRows(rng, dataSize) }]
}

function* customerRows(dataSize: number): Generator<unknown[]> {
  for (let i = 0; i < dataSize; i++) {
    yield customerRow(i + 1)
  }
}

function* productRows(rng: SeededRng, count: number): Generator<unknown[]> {
  for (let i = 0; i < count; i++) {
    yield productRow(rng, i + 1)
  }
}

function tpccSeed(rng: SeededRng, dataSize: number): SeedTable[] {
  const productCount = Math.min(dataSize, 1000)
  return [
    { table: 'customers', columns: ['id', 'name', 'email', 'balance'], rows: customerRows(dataSize) },
    { table: 'products', columns: ['id', 'name', 'price', 'stock'], rows: productRows(rng, productCount) },
  ]
}

const YCSB_READ = 'SELECT * FROM usertable WHERE ycsb_key = ?'
const YCSB_UPDATE = 'UPDATE usertable SET field0 = ? WHERE ycsb_key = ?'

function ycsbKey(ctx: OperationContext): string {
  return `user${ctx.zipf.next(ctx.rng)}`
}

function ycsbValue(ctx: OperationContext): string {
  return `value_${ctx.rng.below(1_000_000)}`
}

function ycsbReadOp(weight: number): Operation {
  return {
    name: 'read',
    weight,
    kind: 'read',
    sqliteSql: YCSB_READ,
    postgresSql: YCSB_READ,
    params: ctx => [ycsbKey(ctx)],
  }
}

function ycsbUpdateOp(weight: number): Operation {
  return {
    name: 'update',
    weight,
    kind: 'write',
    sqliteSql: YCSB_UPDATE,
    postgresSql: YCSB_UPDATE,
    params: ctx => [ycsbValue(ctx), ycsbKey(ctx)],
  }
}

function ycsbRmwOp(weight: number): Operation {
  return {
    name: 'read-modify-write',
    weight,
    kind: 'rmw',
    sqliteSql: YCSB_READ,
    postgresSql: YCSB_READ,
    writeSqliteSql: YCSB_UPDATE,
    writePostgresSql: YCSB_UPDATE,
    params: ctx => [ycsbKey(ctx), ycsbValue(ctx)],
  }
}

function ycsbWorkload(name: string, operations: Operation[]): Workload {
  return {
    name,
    category: 'ycsb',
    tables: ['usertable'],
    sqliteSchema: YCSB_SCHEMA,
    postgresSchema: YCSB_SCHEMA,
    seed: ycsbSeed,
    operations,
  }
}

function pointSelectParams(ctx: OperationContext): unknown[] {
  return [ctx.zipf.next(ctx.rng) + 1]
}

function bulkInsertParams(ctx: OperationContext): unknown[] {
  const rowId = ctx.rng.below(ctx.dataSize) + 1
  return userRow(ctx.rng, rowId)
}

function batchUpdateParams(ctx: OperationContext): unknown[] {
  const rowId = ctx.zipf.next(ctx.rng) + 1
  return [ctx.rng.below(80) + 18, rowId]
}

function newOrderParams(ctx: OperationContext): unknown[] {
  const customerId = ctx.rng.below(ctx.dataSize) + 1
  const total = Math.round(ctx.rng.fraction() * 500 * 100) / 100
  return [customerId, total, 'pending', FIXED_TIMESTAMP]
}

function paymentParams(ctx: OperationContext): unknown[] {
  const amount = Math.round(ctx.rng.fraction() * 100 * 100) / 100
  const customerId = ctx.rng.below(ctx.dataSize) + 1
  return [amount, customerId]
}

export function buildWorkloads(): Map<string, Workload> {
  const catalogue: Workload[] = [
    {
      name: 'point-select',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: userSeed,
      operations: [
        {
          name: 'point-select',
          weight: 1.0,
          kind: 'read',
          sqliteSql: 'SELECT * FROM users WHERE id = ?',
          postgresSql: 'SELECT * FROM users WHERE id = ?',
          params: pointSelectParams,
        },
      ],
    },
    {
      name: 'bulk-insert',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: emptySeed,
      operations: [
        {
          name: 'bulk-insert',
          weight: 1.0,
          kind: 'write',
          sqliteSql: 'INSERT OR REPLACE INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)',
          postgresSql:
            'INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?) ' +
            'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
          params: bulkInsertParams,
        },
      ],
    },
    {
      name: 'batch-update',
      category: 'micro',
      tables: ['users'],
      sqliteSchema: MICRO_SCHEMA,
      postgresSchema: MICRO_SCHEMA,
      seed: userSeed,
      operations: [
        {
          name: 'batch-update',
          weight: 1.0,
          kind: 'write',
          sqliteSql: 'UPDATE users SET age = ? WHERE id = ?',
          postgresSql: 'UPDATE users SET age = ? WHERE id = ?',
          params: batchUpdateParams,
        },
      ],
    },
    ycsbWorkload('ycsb-a', [ycsbReadOp(0.5), ycsbUpdateOp(0.5)]),
    ycsbWorkload('ycsb-b', [ycsbReadOp(0.95), ycsbUpdateOp(0.05)]),
    ycsbWorkload('ycsb-c', [ycsbReadOp(1.0)]),
    ycsbWorkload('ycsb-f', [ycsbReadOp(0.5), ycsbRmwOp(0.5)]),
    {
      name: 'tpc-c-derived',
      category: 'oltp',
      tables: ['orders', 'products', 'customers'],
      sqliteSchema: TPCC_SCHEMA_SQLITE,
      postgresSchema: TPCC_SCHEMA_POSTGRES,
      seed: tpccSeed,
      operations: [
        {
          name: 'new-order',
          weight: 0.5,
          kind: 'write',
          sqliteSql: 'INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)',
          postgresSql: 'INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)',
          params: newOrderParams,
        },
        {
          name: 'payment',
          weight: 0.5,
          kind: 'write',
          sqliteSql: 'UPDATE customers SET balance = balance - ? WHERE id = ?',
          postgresSql: 'UPDATE customers SET balance = balance - ? WHERE id = ?',
          params: paymentParams,
        },
      ],
    },
  ]
  return new Map(catalogue.map(workload => [workload.name, workload]))
}

export function pickOperation(rng: SeededRng, operations: Operation[]): Operation {
  const draw = rng.fraction()
  let cumulative = 0.0
  for (const operation of operations) {
    cumulative += operation.weight
    if (draw < cumulative) {
      return operation
    }
  }
  return operations[operations.length - 1] as Operation
}
