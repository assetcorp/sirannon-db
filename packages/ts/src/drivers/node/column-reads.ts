import { createRequire } from 'node:module'
import type { StatementSync } from 'node:sqlite'
import {
  type ColumnDetail,
  type ColumnRead,
  type ColumnReadLimits,
  limitsRows,
  RowCollector,
  unlimitedRows,
} from '../../core/driver/column-reads.js'
import { bindableParams } from '../../core/driver/values.js'

interface NodeColumn {
  name: string
  type: string | null
  table: string | null
  column: string | null
  database: string | null
}

interface StatementWithColumns {
  columns(): NodeColumn[]
  setReturnArrays(enabled: boolean): void
  all(...params: unknown[]): unknown[]
  iterate(...params: unknown[]): IterableIterator<unknown>
}

/**
 * Returns whether this Node.js release's `node:sqlite` reports each column's details and returns rows as arrays,
 * which needs both `StatementSync.columns` and `StatementSync.setReturnArrays`. Node.js 24 has both, and so does Node.js
 * 22.23.5, while earlier Node.js 22 releases lack at least one of them.
 *
 * @returns `true` when both calls exist.
 * @internal
 */
export function nodeReportsColumnDetails(): boolean {
  try {
    const prototype = createRequire(import.meta.url)('node:sqlite').StatementSync?.prototype
    return typeof prototype?.columns === 'function' && typeof prototype?.setReturnArrays === 'function'
  } catch {
    return false
  }
}

function columnDetailsOf(stmt: StatementWithColumns): ColumnDetail[] {
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
 * the read carries a limit and reading every row in one call while it carries none. When that one call returns rows
 * narrower than the statement's columns, as node:sqlite in Node.js 22 does after a schema change, it reads the rows
 * again one at a time, so that every row holds one value for each column.
 *
 * @param statement - The node:sqlite statement, which reads integers as `bigint`.
 * @param limits - The row and byte limits of the read.
 * @param params - The values to bind.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @internal
 */
export function readWithColumns(
  statement: StatementSync,
  limits: ColumnReadLimits,
  params: readonly unknown[],
): ColumnRead {
  const stmt = statement as unknown as StatementWithColumns
  const bound = bindableParams(params)
  stmt.setReturnArrays(true)
  try {
    if (!limitsRows(limits)) {
      const rows = stmt.all(...bound) as unknown[][]
      const columns = columnDetailsOf(stmt)
      if (rows.length === 0 || rows[0].length === columns.length) {
        return { columns, rows: unlimitedRows(rows, limits), truncated: null }
      }
      const stepped = unlimitedRows([...(stmt.iterate(...bound) as IterableIterator<unknown[]>)], limits)
      return { columns: columnDetailsOf(stmt), rows: stepped, truncated: null }
    }
    const collector = new RowCollector(limits)
    for (const row of stmt.iterate(...bound) as IterableIterator<unknown[]>) {
      if (!collector.accept(row)) break
    }
    return { columns: columnDetailsOf(stmt), rows: collector.rows, truncated: collector.truncated }
  } finally {
    stmt.setReturnArrays(false)
  }
}
