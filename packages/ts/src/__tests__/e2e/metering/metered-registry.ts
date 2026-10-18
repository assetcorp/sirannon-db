import { join } from 'node:path'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, QueryMetrics, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import { trackedRegistry } from '../lib/served-registry.js'
import { readInSeparateProcess } from './oracle.js'

export const SIRANNON_SQLITE_BINDING = builtSirannonSqliteBindingPath()

export const ORDERS_SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)',
  'CREATE INDEX orders_by_total ON orders (total)',
]
export const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
export const SCAN_BY_CUSTOMER = 'SELECT id, customer, total FROM orders WHERE customer = ?'
export const ORDER_BY_ID = 'SELECT id, customer, total FROM orders WHERE id = ?'
export const SET_TOTAL_BY_ID = 'UPDATE orders SET total = ? WHERE id = ?'

export interface Topology {
  label: string
  options: DatabaseOptions
}

export const TOPOLOGIES: readonly Topology[] = [
  { label: 'every statement on the calling thread', options: { writerWorker: false, readPoolSize: 2 } },
  { label: 'writes on the writer worker', options: { writerWorker: true, readPoolSize: 2 } },
  { label: 'writes on the writer worker and one read connection', options: { writerWorker: true, readPoolSize: 1 } },
]

export interface Counts {
  rowsRead: number
  rowsWritten: number
}

export interface Billed extends Counts {
  sql: string
  error: boolean
}

export class Meter {
  readonly reports: QueryMetrics[] = []

  readonly record = (metrics: QueryMetrics): void => {
    this.reports.push(metrics)
  }

  clear(): void {
    this.reports.length = 0
  }

  of(databaseId: string): QueryMetrics[] {
    return this.reports.filter(metrics => metrics.databaseId === databaseId)
  }

  forSql(databaseId: string, sql: string): QueryMetrics[] {
    return this.of(databaseId).filter(metrics => metrics.sql === sql)
  }

  billed(databaseId: string): Billed[] {
    return this.of(databaseId).map(metrics => ({
      sql: metrics.sql,
      error: metrics.error === true,
      rowsRead: metrics.rowsRead ?? Number.NaN,
      rowsWritten: metrics.rowsWritten ?? Number.NaN,
    }))
  }

  bill(databaseId: string): Counts {
    const total: Counts = { rowsRead: 0, rowsWritten: 0 }
    for (const metrics of this.of(databaseId)) {
      total.rowsRead += metrics.rowsRead ?? Number.NaN
      total.rowsWritten += metrics.rowsWritten ?? Number.NaN
    }
    return total
  }
}

export function countingDriver(): SQLiteDriver {
  return betterSqlite3({ rowCounts: true, sqliteBindingPath: SIRANNON_SQLITE_BINDING ?? undefined })
}

export function meteredRegistry(meter: Meter, options: Partial<SirannonOptions> = {}): Sirannon {
  return trackedRegistry({ driver: countingDriver(), ...options, metrics: { onQueryComplete: meter.record } })
}

export interface Tenant {
  id: string
  db: Database
  path: string
}

export async function openOrders(
  sirannon: Sirannon,
  dir: string,
  id: string,
  orders: number,
  options: DatabaseOptions = {},
): Promise<Tenant> {
  const path = join(dir, `${id}.db`)
  const db = await sirannon.open(id, path, options)
  for (const statement of ORDERS_SCHEMA) await db.execute(statement)
  await db.bulkLoad(
    INSERT_ORDER,
    Array.from({ length: orders }, (_, index) => [`${id}-customer-${index}`, index % 10]),
  )
  return { id, db, path }
}

export async function rowsIn(path: string, table: string): Promise<number> {
  const [row] = await readInSeparateProcess<{ n: bigint }>(path, `SELECT count(*) AS n FROM "${table}"`)
  return Number(row?.n ?? -1n)
}

export async function indexesOn(path: string, table: string): Promise<number> {
  const [row] = await readInSeparateProcess<{ n: bigint }>(path, 'SELECT count(*) AS n FROM pragma_index_list(?)', [
    table,
  ])
  return Number(row?.n ?? -1n)
}

export async function scansWholeTable(path: string, sql: string, params: unknown[], table: string): Promise<boolean> {
  const plan = await readInSeparateProcess<{ detail: string }>(path, `EXPLAIN QUERY PLAN ${sql}`, params)
  return plan.length === 1 && plan[0]?.detail === `SCAN ${table}`
}

export async function wholeTableScan(path: string, table: string): Promise<Counts> {
  const rows = await rowsIn(path, table)
  if (rows < 1) throw new Error(`A scan of the empty table ${table} counts its first move, so the oracle needs rows`)
  return { rowsRead: rows, rowsWritten: 0 }
}

export async function rowInserts(path: string, table: string, rows = 1): Promise<Counts> {
  return { rowsRead: 0, rowsWritten: rows * (1 + (await indexesOn(path, table))) }
}

export const ROWID_LOOKUP: Counts = { rowsRead: 1, rowsWritten: 0 }

export const NOTHING: Counts = { rowsRead: 0, rowsWritten: 0 }

export function plusCounts(...parts: Counts[]): Counts {
  return parts.reduce((total, part) => ({
    rowsRead: total.rowsRead + part.rowsRead,
    rowsWritten: total.rowsWritten + part.rowsWritten,
  }))
}

export function billedAs(sql: string, error: boolean, counts: Counts): Billed {
  return { sql, error, rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten }
}

export function leaksNoCounts(text: string): boolean {
  return !/rowsRead|rowsWritten/.test(text)
}
