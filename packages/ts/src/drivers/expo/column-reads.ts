import type { SQLiteBindParams } from 'expo-sqlite'
import {
  type ColumnDetail,
  type ColumnRead,
  type ColumnReadLimits,
  limitsRows,
  RowCollector,
  unlimitedRows,
} from '../../core/driver/column-reads.js'
import { expoBindParams } from './bind.js'

interface ExpoRawResult extends AsyncIterable<unknown[]> {
  getAllAsync(): Promise<unknown[][]>
}

interface ExpoStatement {
  getColumnNamesAsync(): Promise<string[]>
  executeForRawResultAsync(params: SQLiteBindParams): Promise<ExpoRawResult>
  finalizeAsync(): Promise<void>
}

/**
 * The call of an `expo-sqlite` database handle that {@link readWithColumns} uses.
 *
 * @internal
 */
export interface ExpoStatementSource {
  prepareAsync(sql: string): Promise<unknown>
}

function columnDetailsOf(names: readonly string[]): ColumnDetail[] {
  return names.map(name => ({ name, declaredType: null, table: null, column: null, database: null }))
}

/**
 * Executes a statement through `expo-sqlite` and returns its column names with each row as an array. `expo-sqlite`
 * reports no column's declared type or source, so each of those is `null`. A read without limits takes every row in
 * one call, and a read with a limit steps one row at a time and stops at the limits.
 *
 * @param handle - The open `expo-sqlite` database handle.
 * @param sql - The statement text.
 * @param params - The values to bind.
 * @param limits - The row and byte limits of the read.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @internal
 */
export async function readWithColumns(
  handle: ExpoStatementSource,
  sql: string,
  params: readonly unknown[],
  limits: ColumnReadLimits,
): Promise<ColumnRead> {
  const stmt = (await handle.prepareAsync(sql)) as ExpoStatement
  try {
    const columns = columnDetailsOf(await stmt.getColumnNamesAsync())
    const result = await stmt.executeForRawResultAsync(expoBindParams(params))
    if (!limitsRows(limits)) {
      return { columns, rows: unlimitedRows(await result.getAllAsync(), limits), truncated: null }
    }
    const collector = new RowCollector(limits)
    for await (const row of result) {
      if (!collector.accept(row)) break
    }
    return { columns, rows: collector.rows, truncated: collector.truncated }
  } finally {
    await stmt.finalizeAsync()
  }
}
