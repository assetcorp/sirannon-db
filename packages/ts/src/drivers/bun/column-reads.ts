/// <reference path="./bun-sqlite.d.ts" />
import type { Statement } from 'bun:sqlite'
import {
  type ColumnDetail,
  type ColumnRead,
  type ColumnReadLimits,
  limitsRows,
  RowCollector,
  unlimitedRows,
} from '../../core/driver/column-reads.js'
import { bindableParams } from '../../core/driver/values.js'
import { SirannonError } from '../../core/errors.js'

const REPEATED_NAMES_MESSAGE =
  'bun:sqlite returns rows one at a time only as objects keyed by column name, so it cannot read a result whose column names repeat under maxRows or maxBytes. Give each column its own name with AS, or read without the limits'

function columnDetailsOf(stmt: Statement): ColumnDetail[] {
  const declared = stmt.declaredTypes
  return stmt.columnNames.map((name, index) => ({
    name,
    declaredType: Array.isArray(declared) ? (declared[index] ?? null) : null,
    table: null,
    column: null,
    database: null,
  }))
}

function holdsRepeatedName(columns: readonly ColumnDetail[]): boolean {
  return new Set(columns.map(column => column.name)).size !== columns.length
}

/**
 * Executes a statement and returns its column names and declared types with each row as an array. bun:sqlite reports
 * no source table, column, or schema, so each of those is `null`. A read without limits takes every row as arrays in
 * one call, and a read with a limit steps one row at a time and orders each row's values by column name.
 *
 * @param stmt - The bun:sqlite statement, which reads integers as `bigint`.
 * @param limits - The row and byte limits of the read.
 * @param params - The values to bind.
 * @returns The columns, the rows, and the limit that stopped the read.
 * @throws A `SirannonError` with `COLUMN_DETAILS_UNSUPPORTED` for a limited read whose column names repeat.
 * @internal
 */
export function readWithColumns(stmt: Statement, limits: ColumnReadLimits, params: readonly unknown[]): ColumnRead {
  const columns = columnDetailsOf(stmt)
  const bound = bindableParams(params)
  if (!limitsRows(limits)) {
    return { columns, rows: unlimitedRows(stmt.values(...bound), limits), truncated: null }
  }
  if (holdsRepeatedName(columns)) throw new SirannonError(REPEATED_NAMES_MESSAGE, 'COLUMN_DETAILS_UNSUPPORTED')
  const names = columns.map(column => column.name)
  const collector = new RowCollector(limits)
  for (const record of stmt.iterate(...bound)) {
    const row = new Array<unknown>(names.length)
    for (let index = 0; index < names.length; index++) row[index] = record[names[index]]
    if (!collector.accept(row)) break
  }
  return { columns, rows: collector.rows, truncated: collector.truncated }
}
