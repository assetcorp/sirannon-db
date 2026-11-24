import type { ConnectionPool } from './connection-pool.js'
import type { ColumnReadLimits } from './driver/column-reads.js'
import { type Metered, rowCountsOf } from './driver/row-counts.js'
import type { SQLiteConnection } from './driver/types.js'
import { SirannonError } from './errors.js'
import { guardedStatement } from './prepared-statements.js'
import { asQueryError, bindParams } from './query-executor.js'
import type { Params, QueryWithColumnsOptions, QueryWithColumnsResult } from './types.js'

const COLUMN_DETAILS_UNSUPPORTED_MESSAGE =
  "This database's driver reports no column details, because its columnDetails capability is false, so queryWithColumns cannot run on it. On node:sqlite the capability needs Node.js 24 or later"

function describeLimit(value: unknown): string {
  if (typeof value === 'number' || typeof value === 'bigint' || typeof value === 'boolean') return String(value)
  if (value === null) return 'null'
  return typeof value
}

function validLimit(name: 'maxRows' | 'maxBytes', value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return value
  throw new SirannonError(
    `${name} must be a non-negative safe integer, and it was ${describeLimit(value)}`,
    'INVALID_READ_LIMIT',
  )
}

/**
 * Returns the limits that a driver applies to a read of {@link DatabaseColumnQueries.queryWithColumns}, after checking the
 * caller's `maxRows` and `maxBytes`.
 *
 * @param options - The caller's options.
 * @param rawIntegers - `true` when the caller encodes the rows for the wire, so the driver can leave integers raw.
 * @returns The limits for the driver.
 * @throws A `SirannonError` with `INVALID_READ_LIMIT` when a limit is not a non-negative safe integer.
 * @internal
 */
export function readLimitsFrom(options: QueryWithColumnsOptions | undefined, rawIntegers: boolean): ColumnReadLimits {
  const maxRows = validLimit('maxRows', options?.maxRows)
  const maxBytes = validLimit('maxBytes', options?.maxBytes)
  return {
    ...(maxRows === undefined ? {} : { maxRows }),
    ...(maxBytes === undefined ? {} : { maxBytes }),
    ...(rawIntegers ? { rawIntegers } : {}),
  }
}

/**
 * Executes a read on one connection and returns the details of each column with each row as an array, with the
 * statement's row counts when the driver reports them.
 *
 * @param conn - The connection that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param limits - The row and byte limits that the driver applies.
 * @param leasedFrom - The pool that leased `conn` through {@link ConnectionPool.leaseReader}, which gets the lease
 * back once the read settles.
 * @returns The columns, the rows, and the limit that stopped the read, with `rowsRead` and `rowsWritten` from a
 * driver that reports `rowCounts`.
 * @throws A `SirannonError` with `COLUMN_DETAILS_UNSUPPORTED` when the driver's statements report no column details.
 * @internal
 */
export async function queryWithColumnsMetered(
  conn: SQLiteConnection,
  sql: string,
  params: Params | undefined,
  limits: ColumnReadLimits,
  leasedFrom?: ConnectionPool,
): Promise<Metered<QueryWithColumnsResult>> {
  try {
    const handle = guardedStatement(conn, sql)
    const stmt = handle instanceof Promise ? await handle : handle
    if (!stmt.allWithColumns) throw new SirannonError(COLUMN_DETAILS_UNSUPPORTED_MESSAGE, 'COLUMN_DETAILS_UNSUPPORTED')
    const read = await stmt.allWithColumns(limits, ...bindParams(params))
    const value: QueryWithColumnsResult = { columns: read.columns, rows: read.rows, truncated: read.truncated }
    const counts = rowCountsOf(read)
    return counts === undefined ? { value } : { value, rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten }
  } catch (err) {
    throw asQueryError(err, sql)
  } finally {
    leasedFrom?.releaseReader()
  }
}
