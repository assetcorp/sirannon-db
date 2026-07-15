// The order-entry workloads over a customers/products/orders schema.
//
// `tpc-c-derived` offers an insert and an update as independent autocommit statements. Nothing
// binds the two together, so it measures the cost of single writes under a mixed statement stream.
//
// `tpc-c-new-order` offers the checkout TPC-C models as its New-Order transaction: record the
// order, take the stock for each line, and charge the customer, all committing or rolling back as
// one unit. This is the workload that reaches the engine's transaction path, so it is the one that
// shows what grouping concurrent commits is worth.
//
// Stock follows TPC-C's own replenishment rule (clause 2.4.2.2): a line that would take a product
// below its remaining quantity restocks it by 91 instead of driving it negative. Without that rule
// a long run leaves every product at an impossible negative quantity and the final database no
// longer passes a correctness check, which is the first thing a reader of a benchmark should be
// able to make.

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

// TPC-C draws 5 to 15 lines per order. A fixed count is used instead so one operation always costs
// the same statements, which keeps the reported ops/sec and its latency distribution a measurement
// of the engine rather than of the draw. Four lines is within the range and is a plausible basket.
const ORDER_LINES = 4
const MAX_LINE_QUANTITY = 5
const STOCK_REPLENISH = 91

const INSERT_ORDER =
  'INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)'
const TAKE_STOCK =
  'UPDATE products SET stock = CASE WHEN stock >= ? THEN stock - ? ELSE stock - ? + ? END WHERE id = ?'
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
              sets.push([quantity, quantity, quantity, STOCK_REPLENISH, ctx.rng.below(productCount) + 1])
            }
            sets.push([total, customerId])
            return sets
          },
        },
      ],
    },
  ]
}
