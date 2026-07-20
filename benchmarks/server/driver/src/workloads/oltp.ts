import type { SeededRng } from '../rng.ts'
import type { SeedTable, StatementTemplate, Workload } from './workload.ts'

const CUSTOMERS_AND_PRODUCTS =
  'CREATE TABLE IF NOT EXISTS customers (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, balance REAL NOT NULL);' +
  'CREATE TABLE IF NOT EXISTS products (' +
  'id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL);'

const ORDER_ENTRY_SCHEMA_SQLITE =
  `${CUSTOMERS_AND_PRODUCTS}CREATE TABLE IF NOT EXISTS orders (` +
  'id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, ' +
  'status TEXT NOT NULL, created_at TEXT NOT NULL)'

const ORDER_ENTRY_SCHEMA_POSTGRES =
  `${CUSTOMERS_AND_PRODUCTS}CREATE TABLE IF NOT EXISTS orders (` +
  'id SERIAL PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, ' +
  'status TEXT NOT NULL, created_at TEXT NOT NULL)'

const ORDER_ENTRY_TABLES = ['orders', 'products', 'customers']
const PRODUCT_COUNT_CAP = 1000
const FIXED_TIMESTAMP = '2026-01-01T00:00:00.000Z'

const ORDER_LINES = 4
const MAX_LINE_QUANTITY = 5
const TPCC_2_4_2_2_RESTOCK_QUANTITY = 91

const INSERT_ORDER = 'INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)'
const TAKE_STOCK = 'UPDATE products SET stock = CASE WHEN stock >= ? THEN stock - ? ELSE stock - ? + ? END WHERE id = ?'
const CHARGE_CUSTOMER = 'UPDATE customers SET balance = balance - ? WHERE id = ?'

function bothDialects(sql: string): StatementTemplate {
  return { sqliteSql: sql, postgresSql: sql }
}

function newOrderStatements(): StatementTemplate[] {
  const statements = [bothDialects(INSERT_ORDER)]
  for (let line = 0; line < ORDER_LINES; line++) {
    statements.push(bothDialects(TAKE_STOCK))
  }
  statements.push(bothDialects(CHARGE_CUSTOMER))
  return statements
}

function customerRow(rowId: number): unknown[] {
  return [rowId, `Customer ${rowId}`, `customer${rowId}@store.com`, 10000.0]
}

function productRow(rng: SeededRng, rowId: number): unknown[] {
  const price = Math.round((10 + rng.fraction() * 490) * 100) / 100
  return [rowId, `Product ${rowId}`, price, 1000]
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

function orderEntrySeed(rng: SeededRng, dataSize: number): SeedTable[] {
  return [
    { table: 'customers', columns: ['id', 'name', 'email', 'balance'], rows: customerRows(dataSize) },
    {
      table: 'products',
      columns: ['id', 'name', 'price', 'stock'],
      rows: productRows(rng, Math.min(dataSize, PRODUCT_COUNT_CAP)),
    },
  ]
}

export function oltpWorkloads(): Workload[] {
  return [
    {
      name: 'tpc-c-derived',
      category: 'oltp',
      tables: ORDER_ENTRY_TABLES,
      sqliteSchema: ORDER_ENTRY_SCHEMA_SQLITE,
      postgresSchema: ORDER_ENTRY_SCHEMA_POSTGRES,
      seed: orderEntrySeed,
      operations: [
        {
          name: 'record-order',
          weight: 0.5,
          kind: 'write',
          sqliteSql: INSERT_ORDER,
          postgresSql: INSERT_ORDER,
          params: ctx => [
            ctx.rng.below(ctx.dataSize) + 1,
            Math.round(ctx.rng.fraction() * 500 * 100) / 100,
            'pending',
            FIXED_TIMESTAMP,
          ],
        },
        {
          name: 'charge-customer',
          weight: 0.5,
          kind: 'write',
          sqliteSql: CHARGE_CUSTOMER,
          postgresSql: CHARGE_CUSTOMER,
          params: ctx => [Math.round(ctx.rng.fraction() * 100 * 100) / 100, ctx.rng.below(ctx.dataSize) + 1],
        },
      ],
    },
    {
      name: 'tpc-c-new-order',
      category: 'oltp',
      tables: ORDER_ENTRY_TABLES,
      sqliteSchema: ORDER_ENTRY_SCHEMA_SQLITE,
      postgresSchema: ORDER_ENTRY_SCHEMA_POSTGRES,
      seed: orderEntrySeed,
      operations: [
        {
          name: 'new-order',
          weight: 1.0,
          kind: 'transaction',
          statements: newOrderStatements(),
          params: ctx => {
            const customerId = ctx.rng.below(ctx.dataSize) + 1
            const total = Math.round(ctx.rng.fraction() * 500 * 100) / 100
            const sets: unknown[][] = [[customerId, total, 'pending', FIXED_TIMESTAMP]]
            const productCount = Math.min(ctx.dataSize, PRODUCT_COUNT_CAP)
            for (let line = 0; line < ORDER_LINES; line++) {
              const quantity = ctx.rng.below(MAX_LINE_QUANTITY) + 1
              sets.push([quantity, quantity, quantity, TPCC_2_4_2_2_RESTOCK_QUANTITY, ctx.rng.below(productCount) + 1])
            }
            sets.push([total, customerId])
            return sets
          },
        },
      ],
    },
  ]
}
