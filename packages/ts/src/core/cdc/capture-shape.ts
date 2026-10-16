import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { parseColumnDefinitions } from '../live/column-definitions.js'
import { tokenizeSql } from '../live/sql-tokens.js'
import {
  type IndexColumnRow,
  type IndexListRow,
  indexColumns,
  primaryKeyColumns,
  selectIndexSql,
  selectTableSql,
  TABLE_XINFO_HIDDEN,
  tableColumnsWithGenerated,
  tableIndexList,
  tableInfoRows,
} from '../system-catalog/index.js'
import { parseIndexDefinition } from './index-definition.js'

const ROWID_COLUMN = -1
const EXPRESSION_COLUMN = -2

/**
 * Names one part of a unique key: a column, or an expression over the columns of the row, with the collation that the key compares it with.
 *
 * @internal
 */
export interface UniqueKeyPart {
  column: string | null
  expression: string | null
  collation: string
}

/**
 * Describes one unique index of a table: the parts of its key, and the condition that a row must meet to enter a partial index.
 *
 * @internal
 */
export interface UniqueKey {
  parts: UniqueKeyPart[]
  where: string | null
}

/**
 * Names a generated column and the expression that SQLite computes it from, or `null` when the table definition does not yield that expression.
 *
 * @internal
 */
export interface GeneratedColumn {
  name: string
  expression: string | null
}

/**
 * Describes what the change-capture triggers of one table record: its columns, its row identity, every unique key whose conflict a `REPLACE` resolves by deleting a row, and the generated columns that such a key can read.
 *
 * @internal
 */
export interface CaptureShape {
  columns: string[]
  allColumns: string[]
  pkColumns: string[]
  uniqueKeys: UniqueKey[]
  generatedColumns: GeneratedColumn[]
  withoutRowid: boolean
}

/**
 * Reads the capture shape of a table from the schema.
 *
 * @param conn - The connection to read the schema through.
 * @param table - The stored name of the table.
 * @returns The shape, whose column list is empty when the table does not exist.
 * @throws {@link CDCError} when SQLite stores a unique index whose key list cannot be read.
 */
export async function readCaptureShape(conn: SQLiteConnection, table: string): Promise<CaptureShape> {
  const info = await tableInfoRows(conn, table)
  const columns = info.map(column => column.name)
  const pkColumns = primaryKeyColumns(info)
  if (columns.length === 0) {
    return { columns, allColumns: [], pkColumns, uniqueKeys: [], generatedColumns: [], withoutRowid: false }
  }

  const xinfo = await tableColumnsWithGenerated(conn, table)
  const allColumns = xinfo.map(column => column.name)
  const uniqueKeys: UniqueKey[] = []
  for (const index of await tableIndexList(conn, table)) {
    if (index.unique !== 1) continue
    const key = await uniqueKeyOf(conn, table, index, await indexColumns(conn, index.name))
    if (key !== null) uniqueKeys.push(key)
  }
  const ddl = await selectTableSql(conn, table)
  const generated = xinfo.filter(
    column =>
      column.hidden === TABLE_XINFO_HIDDEN.generatedVirtual || column.hidden === TABLE_XINFO_HIDDEN.generatedStored,
  )
  const definitions = ddl === null || generated.length === 0 ? null : parseColumnDefinitions(ddl)
  const generatedColumns = generated.map(column => ({
    name: column.name,
    expression: definitions?.get(column.name)?.generatedAs ?? null,
  }))
  const withoutRowid = ddl !== null && declaresWithoutRowid(ddl)
  return { columns, allColumns, pkColumns, uniqueKeys, generatedColumns, withoutRowid }
}

/**
 * Returns whether two capture shapes produce the same triggers.
 *
 * @param a - The first shape.
 * @param b - The second shape.
 * @returns `true` when the columns, row identity, and unique keys all match.
 */
export function sameCaptureShape(a: CaptureShape, b: CaptureShape): boolean {
  return (
    a.withoutRowid === b.withoutRowid &&
    sameList(a.columns, b.columns) &&
    sameList(a.allColumns, b.allColumns) &&
    sameList(a.pkColumns, b.pkColumns) &&
    sameList(a.uniqueKeys.map(keySignature), b.uniqueKeys.map(keySignature)) &&
    sameList(a.generatedColumns.map(generatedSignature), b.generatedColumns.map(generatedSignature))
  )
}

async function uniqueKeyOf(
  conn: SQLiteConnection,
  table: string,
  index: IndexListRow,
  rows: readonly IndexColumnRow[],
): Promise<UniqueKey | null> {
  const keyRows = rows.filter(row => row.key === 1)
  if (keyRows.length === 0 || keyRows.some(row => row.cid === ROWID_COLUMN)) return null
  const needsDefinition = index.partial === 1 || keyRows.some(row => row.cid === EXPRESSION_COLUMN)
  const sql = needsDefinition ? await selectIndexSql(conn, index.name) : null
  const definition = sql === null ? null : parseIndexDefinition(sql)
  if (needsDefinition && (definition === null || definition.elements.length !== keyRows.length)) {
    throw new CDCError(`Cannot watch '${table}': the key list of its unique index '${index.name}' could not be read`)
  }
  const parts = keyRows.map((row, position) => ({
    column: row.cid === EXPRESSION_COLUMN ? null : row.name,
    expression: row.cid === EXPRESSION_COLUMN ? (definition?.elements[position] ?? null) : null,
    collation: row.collation,
  }))
  return { parts, where: definition?.where ?? null }
}

function declaresWithoutRowid(ddl: string): boolean {
  const tokens = tokenizeSql(ddl)
  let closed = false
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.kind === 'punct' && token.value === ')' && token.depth === 0) {
      closed = true
      continue
    }
    if (!closed || token.kind !== 'word' || token.quoted || token.lower !== 'without') continue
    const next = tokens[i + 1]
    if (next !== undefined && next.kind === 'word' && !next.quoted && next.lower === 'rowid') return true
  }
  return false
}

function keySignature(key: UniqueKey): string {
  const parts = key.parts.map(part => `${part.column ?? ''}\u0000${part.expression ?? ''}\u0000${part.collation}`)
  return `${parts.join('\u0001')}\u0002${key.where ?? ''}`
}

function generatedSignature(column: GeneratedColumn): string {
  return `${column.name}\u0000${column.expression ?? '\u0001'}`
}

function sameList(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((value, index) => value === b[index])
}
