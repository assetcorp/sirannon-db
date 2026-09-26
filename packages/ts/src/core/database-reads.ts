import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseObserver } from './database-observability.js'
import type { SQLiteConnection } from './driver/types.js'
import type { QueryOutcomeMeasure } from './metrics/collector.js'
import { query, queryForWire, queryOne } from './query-executor.js'
import type { Params, QueryOptions } from './types.js'
import type { WriterLock } from './writer-lock.js'

export interface DatabaseReadDeps {
  pool: ConnectionPool
  writerLock: WriterLock
  observer: DatabaseObserver
}

function countRows(rows: readonly unknown[]): { rowsReturned: number } {
  return { rowsReturned: rows.length }
}

function countRow(row: unknown): { rowsReturned: number } {
  return { rowsReturned: row === undefined ? 0 : 1 }
}

export function readRows<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T[]> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => query<T>(conn, sql, params), countRows),
  )
}

export function readWireRows(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryForWire(conn, sql, params), countRows),
  )
}

export function readOneRow<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T | undefined> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryOne<T>(conn, sql, params), countRow),
  )
}

function onReadConnection<T>(
  deps: DatabaseReadDeps,
  sql: string,
  op: (conn: SQLiteConnection) => Promise<T>,
  measure: QueryOutcomeMeasure<T>,
): Promise<T> {
  if (deps.pool.readerCount === 0) {
    return deps.writerLock.run(() => deps.observer.track(sql, () => op(deps.pool.acquireWriter()), measure))
  }
  return deps.observer.track(sql, () => op(deps.pool.acquireReader()), measure)
}
