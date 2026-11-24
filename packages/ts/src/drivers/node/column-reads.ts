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
 * which needs `StatementSync.columns` from Node.js 22.16 and `StatementSync.setReturnArrays` from Node.js 24.0.
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
 * the read carries a limit and reading every row in one call while it carries none.
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
      const rows = unlimitedRows(stmt.all(...bound) as unknown[][], limits)
      return { columns: columnDetailsOf(stmt), rows, truncated: null }
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
