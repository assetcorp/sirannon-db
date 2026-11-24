import {
  type ColumnDetail,
  type ColumnRead,
  type ColumnReadLimits,
  RowCollector,
} from '../../core/driver/column-reads.js'
import { bindStatement, type WaSqliteBindings, type WaSqliteMemory } from './bind.js'
import { readColumn, type WaSqliteColumns } from './read.js'

const SQLITE_ROW = 100

/**
 * The calls of the wa-sqlite API that {@link readWithColumns} uses.
 *
 * @internal
 */
export interface WaSqliteStatements extends WaSqliteBindings, WaSqliteColumns {
  statements(db: number, sql: string): AsyncIterable<number>
  column_names(stmt: number): string[]
  step(stmt: number): Promise<number>
}

function columnDetailsOf(names: readonly string[]): ColumnDetail[] {
  return names.map(name => ({ name, declaredType: null, table: null, column: null, database: null }))
}

/**
 * Executes the statements in `sql` up to and including the first one that returns columns, and returns that
 * statement's column names with each of its rows as an array, stepping one row at a time and stopping at the read's
 * limits. It executes no statement after that one, so every row holds one value for each column. The wa-sqlite build
 * exports no call for a column's declared type or source, so each of those is `null`.
 *
 * @param sqlite3 - The wa-sqlite API.
 * @param memory - The WebAssembly module that the API runs on.
 * @param db - The open database handle.
 * @param sql - The statement text.
 * @param params - The values to bind.
 * @param limits - The row and byte limits of the read.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @internal
 */
export async function readWithColumns(
  sqlite3: WaSqliteStatements,
  memory: WaSqliteMemory,
  db: number,
  sql: string,
  params: readonly unknown[],
  limits: ColumnReadLimits,
): Promise<ColumnRead> {
  let columns: ColumnDetail[] | undefined
  const collector = new RowCollector(limits)
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length > 0) bindStatement(sqlite3, memory, stmt, params)
    const names = sqlite3.column_names(stmt)
    if (names.length > 0) columns = columnDetailsOf(names)
    while ((await sqlite3.step(stmt)) === SQLITE_ROW) {
      const row = new Array<unknown>(names.length)
      for (let index = 0; index < names.length; index++) row[index] = readColumn(sqlite3, memory, stmt, index)
      if (!collector.accept(row)) break
    }
    if (columns !== undefined) break
  }
  return { columns: columns ?? [], rows: collector.rows, truncated: collector.truncated }
}
