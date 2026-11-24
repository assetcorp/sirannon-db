import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseObserver } from './database-observability.js'
import type { Metered } from './driver/row-counts.js'
import type { SQLiteConnection } from './driver/types.js'
import type { QueryOutcomeMeasure } from './metrics/collector.js'
import { columnReadOutcome, readOneOutcome, readOutcome } from './metrics/outcomes.js'
import { queryWithColumnsMetered, readLimitsFrom } from './query-columns.js'
import { query, queryForWire, queryMetered, queryOne, queryOneMetered, queryRowsForWire } from './query-executor.js'
import type { Params, QueryOptions, QueryWithColumnsOptions, QueryWithColumnsResult } from './types.js'
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
  if (readsUnobserved(deps)) return query<T>(deps.pool.leaseReader(), sql, params, deps.pool)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, (conn, leasedFrom) => queryMetered<T>(conn, sql, params, leasedFrom), readOutcome),
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
  if (readsUnobserved(deps)) return queryRowsForWire(deps.pool.leaseReader(), sql, params, deps.pool)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(deps, sql, (conn, leasedFrom) => queryForWire(conn, sql, params, leasedFrom), readOutcome),
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
  if (readsUnobserved(deps)) return queryOne<T>(deps.pool.leaseReader(), sql, params, deps.pool)
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(
      deps,
      sql,
      (conn, leasedFrom) => queryOneMetered<T>(conn, sql, params, leasedFrom),
      readOneOutcome,
    ),
  )
}

/**
 * Returns the details of each column of a read with every row as an array, stopping at the caller's `maxRows` and
 * `maxBytes`, running it on a reader connection with no hook or metric work when nothing observes queries, and
 * otherwise through the query hooks and the metrics.
 *
 * @param deps - The connection pool, writer lock, and observer of the database.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern that the query hooks receive, and the two limits.
 * @param rawIntegers - `true` when the caller encodes the rows for the wire, so the driver can leave integers raw.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @throws A `SirannonError` with `INVALID_READ_LIMIT` before any work when a limit is not a non-negative safe integer.
 * @internal
 */
export function readColumnRows(
  deps: DatabaseReadDeps,
  sql: string,
  params: Params | undefined,
  options: QueryWithColumnsOptions | undefined,
  rawIntegers: boolean,
): Promise<QueryWithColumnsResult> {
  const limits = readLimitsFrom(options, rawIntegers)
  if (readsUnobserved(deps)) {
    return queryWithColumnsMetered(deps.pool.leaseReader(), sql, params, limits, deps.pool).then(read => read.value)
  }
  return deps.observer.withQueryHooks(sql, params, options, () =>
    onReadConnection(
      deps,
      sql,
      (conn, leasedFrom) => queryWithColumnsMetered(conn, sql, params, limits, leasedFrom),
      columnReadOutcome,
    ),
  )
}

function readsUnobserved(deps: DatabaseReadDeps): boolean {
  return deps.pool.readerCount > 0 && !deps.observer.observesQueries
}

async function onReadConnection<T>(
  deps: DatabaseReadDeps,
  sql: string,
  op: (conn: SQLiteConnection, leasedFrom?: ConnectionPool) => Promise<Metered<T>>,
  measure: QueryOutcomeMeasure<Metered<T>>,
): Promise<T> {
  const refusal = deps.pool.readerCount === 0 ? deps.writerLock.callerRefusal() : null
  if (refusal !== null) throw refusal
  const metered =
    deps.pool.readerCount === 0
      ? await deps.writerLock.run(() => deps.observer.track(sql, () => op(deps.pool.acquireWriter()), measure))
      : await deps.observer.track(sql, () => op(deps.pool.leaseReader(), deps.pool), measure)
  return metered.value
}
