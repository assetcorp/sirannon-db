import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseObserver } from './database-observability.js'
import type { Metered } from './driver/row-counts.js'
import type { SQLiteConnection } from './driver/types.js'
import type { QueryOutcomeMeasure } from './metrics/collector.js'
import { readOneOutcome, readOutcome } from './metrics/outcomes.js'
import { queryForWire, queryMetered, queryOneMetered } from './query-executor.js'
import type { Params, QueryOptions } from './types.js'
import type { WriterLock } from './writer-lock.js'

export interface DatabaseReadDeps {
  pool: ConnectionPool
  writerLock: WriterLock
  observer: DatabaseObserver
}

export function readRows<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T[]> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryMetered<T>(conn, sql, params), readOutcome),
  )
}

export function readWireRows(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryForWire(conn, sql, params), readOutcome),
  )
}

export function readOneRow<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T | undefined> {
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryOneMetered<T>(conn, sql, params), readOneOutcome),
  )
}

async function onReadConnection<T>(
  deps: DatabaseReadDeps,
  sql: string,
  op: (conn: SQLiteConnection) => Promise<Metered<T>>,
  measure: QueryOutcomeMeasure<Metered<T>>,
): Promise<T> {
  const metered =
    deps.pool.readerCount === 0
      ? await deps.writerLock.run(() => deps.observer.track(sql, () => op(deps.pool.acquireWriter()), measure))
      : await deps.observer.track(sql, () => op(deps.pool.acquireReader()), measure)
  return metered.value
}
