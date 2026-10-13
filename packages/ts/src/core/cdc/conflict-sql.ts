import { CDCError } from '../errors.js'
import type { CaptureShape, GeneratedColumn, UniqueKey } from './capture-shape.js'
import { rewriteColumnReferences } from './index-definition.js'

const ROWID_NAMES = ['rowid', 'oid', '_rowid_'] as const

/**
 * Holds the SQL that finds the stored rows which a new row collides with: one condition per rowid or unique key, each written over the table and `NEW`, and a token built from the unique-key values of `NEW`, which the `BEFORE` and `AFTER` triggers of one write compute alike.
 *
 * @internal
 */
export interface ConflictSql {
  matches: string[]
  token: string | null
}

/**
 * Returns the conditions that find every stored row which a new row collides with on its rowid or on one of its unique keys, and the token that names the write.
 *
 * @param table - The watched table.
 * @param shape - The capture shape of the table.
 * @returns The conditions, and a `null` token for a table whose only key is its rowid.
 * @internal
 */
export function conflictSql(table: string, shape: CaptureShape): ConflictSql {
  const matches = shape.withoutRowid ? [] : [`"${table}".rowid = NEW.rowid`]
  const values: string[] = []
  for (const key of shape.uniqueKeys) {
    const lookup = keyLookup(table, shape, key)
    matches.push(lookup.match)
    values.push(...lookup.values)
  }
  return { matches, token: values.length === 0 ? null : values.join(" || ',' || ") }
}

/**
 * Returns a condition that holds when an update changes the rowid or any stored column that the primary key or a unique key is computed from, so that only such an update pays for the conflict lookups.
 *
 * @param table - The watched table.
 * @param shape - The capture shape of the table.
 * @returns The condition over `NEW` and `OLD`, which reads no generated column, because SQLite leaves generated columns of `NEW` unset in a `BEFORE UPDATE` trigger.
 * @throws {@link CDCError} when a key reads a generated column whose expression the table definition does not yield.
 * @internal
 */
export function keyChangedCondition(table: string, shape: CaptureShape): string {
  const parts = shape.withoutRowid ? [] : ['NEW.rowid IS NOT OLD.rowid']
  for (const column of storedKeySources(table, shape)) {
    parts.push(`NEW."${escId(column)}" IS NOT OLD."${escId(column)}" COLLATE BINARY`)
  }
  return parts.join(' OR ')
}

/**
 * Returns the column list of an `UPDATE OF` trigger that fires on every update able to change the rowid, the primary key, or a unique key, so that SQLite skips the trigger, and keeps its one-pass update, when the `SET` list names none of them.
 *
 * @param table - The watched table.
 * @param shape - The capture shape of the table.
 * @returns The quoted column names, joined by commas, with the rowid names first on a rowid table and each generated column replaced by the stored columns that it is computed from.
 * @throws {@link CDCError} when a key reads a generated column whose expression the table definition does not yield.
 * @internal
 */
export function keyUpdateColumnList(table: string, shape: CaptureShape): string {
  const named = new Map<string, string>()
  if (!shape.withoutRowid) for (const alias of ROWID_NAMES) named.set(alias, alias)
  for (const column of storedKeySources(table, shape)) named.set(column.toLowerCase(), column)
  return [...named.values()].map(column => `"${escId(column)}"`).join(', ')
}

/**
 * Returns the expression that identifies a stored row in the replaced-rows table: the rowid of a rowid table, or the primary key of a `WITHOUT ROWID` table.
 *
 * @param shape - The capture shape of the table.
 * @param ref - The row reference, such as `NEW`, `OLD`, or the quoted table name.
 * @returns The expression.
 * @internal
 */
export function rowKey(shape: CaptureShape, ref: string): string {
  return shape.withoutRowid ? pkRef(shape.pkColumns, ref) : `${ref}.rowid`
}

/**
 * Returns the expression that the change log stores as the row id of a row: its primary key, its key columns joined by `-`, or its rowid.
 *
 * @param pkColumns - The primary-key columns in key order.
 * @param ref - The row reference.
 * @returns The expression.
 * @internal
 */
export function pkRef(pkColumns: readonly string[], ref: string): string {
  if (pkColumns.length === 0) return `${ref}.rowid`
  if (pkColumns.length === 1) return `${ref}."${escId(pkColumns[0])}"`
  return pkColumns.map(column => `${ref}."${escId(column)}"`).join(" || '-' || ")
}

/**
 * Doubles every double quote in an identifier, so that it can sit inside double quotes in SQL.
 *
 * @param name - The identifier.
 * @returns The escaped identifier.
 * @internal
 */
export function escId(name: string): string {
  return name.replace(/"/g, '""')
}

function keyLookup(table: string, shape: CaptureShape, key: UniqueKey): { match: string; values: string[] } {
  const terms: string[] = []
  const values: string[] = []
  for (const part of key.parts) {
    const stored = part.column !== null ? `"${table}"."${escId(part.column)}"` : `(${part.expression})`
    const incoming =
      part.column !== null
        ? newValue(table, shape, part.column, [])
        : `(${newSide(table, shape, part.expression ?? 'NULL')})`
    terms.push(`${stored} = ${incoming} COLLATE "${escId(part.collation)}"`)
    values.push(`quote(${incoming})`)
  }
  if (key.where !== null) terms.push(`(${key.where})`, `(${newSide(table, shape, key.where)})`)
  return { match: terms.join(' AND '), values }
}

function newSide(table: string, shape: CaptureShape, expression: string, within: readonly string[] = []): string {
  return rewriteColumnReferences(expression, shape.allColumns, column => newValue(table, shape, column, within)).sql
}

function newValue(table: string, shape: CaptureShape, column: string, within: readonly string[]): string {
  const generated = shape.generatedColumns.find(candidate => candidate.name === column)
  if (generated === undefined) return `NEW."${escId(column)}"`
  return `(${newSide(table, shape, generatedExpression(table, generated, within), [...within, column])})`
}

function storedKeySources(table: string, shape: CaptureShape): string[] {
  const pending = [...keyColumns(shape)]
  const stored: string[] = []
  const expanded = new Set<string>()
  for (let at = 0; at < pending.length; at++) {
    const column = pending[at]
    if (expanded.has(column)) continue
    expanded.add(column)
    const generated = shape.generatedColumns.find(candidate => candidate.name === column)
    if (generated === undefined) stored.push(column)
    else pending.push(...reads(generatedExpression(table, generated, []), shape))
  }
  return stored
}

function generatedExpression(table: string, column: GeneratedColumn, within: readonly string[]): string {
  if (column.expression === null || within.includes(column.name)) {
    throw new CDCError(
      `Cannot watch '${table}': the expression of generated column '${column.name}', which one of its unique keys reads, could not be read from the table definition`,
    )
  }
  return column.expression
}

function keyColumns(shape: CaptureShape): Set<string> {
  const read = new Set(shape.pkColumns)
  for (const key of shape.uniqueKeys) {
    for (const part of key.parts) {
      if (part.column !== null) read.add(part.column)
      if (part.expression !== null) for (const column of reads(part.expression, shape)) read.add(column)
    }
    if (key.where !== null) for (const column of reads(key.where, shape)) read.add(column)
  }
  return read
}

function reads(expression: string, shape: CaptureShape): Set<string> {
  return rewriteColumnReferences(expression, shape.allColumns, 'NEW').columns
}
