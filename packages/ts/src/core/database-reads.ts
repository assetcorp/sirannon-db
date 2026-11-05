import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseObserver } from './database-observability.js'
import type { Metered } from './driver/row-counts.js'
import type { SQLiteConnection } from './driver/types.js'
import type { QueryOutcomeMeasure } from './metrics/collector.js'
import { readOneOutcome, readOutcome } from './metrics/outcomes.js'
import { query, queryForWire, queryMetered, queryOne, queryOneMetered, queryRowsForWire } from './query-executor.js'
import type { Params, QueryOptions } from './types.js'
import type { WriterLock } from './writer-lock.js'

export interface DatabaseReadDeps {
  pool: ConnectionPool
  writerLock: WriterLock
  observer: DatabaseObserver
}

/**
 * Returns the rows of a read, running it on a reader connection with no hook or metric work when nothing observes
 * queries, and otherwise through the query hooks and the metrics.
 *
 * @param deps - The connection pool, writer lock, and observer of the database.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern that the query hooks receive.
 * @returns The rows with their values decoded.
 * @internal
 */
export function readRows<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T[]> {
  if (readsUnobserved(deps)) return query<T>(deps.pool.acquireReader(), sql, params)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryMetered<T>(conn, sql, params), readOutcome),
  )
}

/**
 * Returns the rows of a read encoded for a network response, running it on a reader connection with no hook or
 * metric work when nothing observes queries, and otherwise through the query hooks and the metrics.
 *
 * @param deps - The connection pool, writer lock, and observer of the database.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern that the query hooks receive.
 * @returns The rows with every value encoded for the wire.
 * @internal
 */
export function readWireRows(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  if (readsUnobserved(deps)) return queryRowsForWire(deps.pool.acquireReader(), sql, params)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryForWire(conn, sql, params), readOutcome),
  )
}

/**
 * Returns the first row of a read, running it on a reader connection with no hook or metric work when nothing
 * observes queries, and otherwise through the query hooks and the metrics.
 *
 * @param deps - The connection pool, writer lock, and observer of the database.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern that the query hooks receive.
 * @returns The first row, or undefined when the statement returns none.
 * @internal
 */
export function readOneRow<T>(
  deps: DatabaseReadDeps,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<T | undefined> {
  if (readsUnobserved(deps)) return queryOne<T>(deps.pool.acquireReader(), sql, params)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, conn => queryOneMetered<T>(conn, sql, params), readOneOutcome),
  )
}

function readsUnobserved(deps: DatabaseReadDeps): boolean {
  return deps.pool.readerCount > 0 && !deps.observer.observesQueries
}

async function onReadConnection<T>(
  deps: DatabaseReadDeps,
  sql: string,
  op: (conn: SQLiteConnection) => Promise<Metered<T>>,
  measure: QueryOutcomeMeasure<Metered<T>>,
): Promise<T> {
  const refusal = deps.pool.readerCount === 0 ? deps.writerLock.callerRefusal() : null
  if (refusal !== null) throw refusal
  const metered =
    deps.pool.readerCount === 0
      ? await deps.writerLock.run(() => deps.observer.track(sql, () => op(deps.pool.acquireWriter()), measure))
      : await deps.observer.track(sql, () => op(deps.pool.acquireReader()), measure)
  return metered.value
}
