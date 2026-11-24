import type BetterSqlite3 from 'better-sqlite3'
import {
  type ColumnDetail,
  type ColumnRead,
  type ColumnReadLimits,
  limitsRows,
  RowCollector,
  unlimitedRows,
} from '../../core/driver/column-reads.js'
import { bindableParams } from '../../core/driver/values.js'

const STATEMENT_RETURNS_NO_DATA = 'This statement does not return data. Use run() instead'

function columnDetailsOf(stmt: BetterSqlite3.Statement): ColumnDetail[] {
  return stmt.columns().map(column => ({
    name: column.name,
    declaredType: column.type ?? null,
    table: column.table ?? null,
    column: column.column ?? null,
    database: column.database ?? null,
  }))
}

/**
 * Executes a statement and returns its column details with each row as an array, stepping one row at a time while
 * the read carries a limit and reading every row in one call while it carries none.
 *
 * @param stmt - The better-sqlite3 statement, which reads integers as `bigint`.
 * @param limits - The row and byte limits of the read.
 * @param params - The values to bind.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @throws TypeError when the statement returns no data, with the message that better-sqlite3 raises for `all`.
 * @internal
 */
export function readWithColumns(
  stmt: BetterSqlite3.Statement,
  limits: ColumnReadLimits,
  params: readonly unknown[],
): ColumnRead {
  if (!stmt.reader) throw new TypeError(STATEMENT_RETURNS_NO_DATA)
  const columns = columnDetailsOf(stmt)
  const bound = bindableParams(params)
  stmt.raw(true)
  try {
    if (!limitsRows(limits)) {
      return { columns, rows: unlimitedRows(stmt.all(...bound) as unknown[][], limits), truncated: null }
    }
    const collector = new RowCollector(limits)
    for (const row of stmt.iterate(...bound) as IterableIterator<unknown[]>) {
      if (!collector.accept(row)) break
    }
    return { columns, rows: collector.rows, truncated: collector.truncated }
  } finally {
    stmt.raw(false)
  }
}
