import { CDCError } from '../errors.js'
import { INTERNAL_TABLE_PREFIX } from '../internal-tables.js'
import type { LiveProbeColumn } from '../system-catalog/live-probe-table.js'
import { selectLiveProbeMatchesSql } from '../system-catalog/live-probe-table.js'
import type { Params } from '../types.js'
import { parameterName, tokenizeSql } from './sql-tokens.js'
import { buildSortKeyPlan, type SortKeyPlan } from './sqlite-order.js'
import type { SelectItem, SortTerm, StatementShape, WindowBound } from './statement-shape.js'

export const KEY_COLUMN = '_sirannon_k'
export const SORT_COLUMN_PREFIX = '_sirannon_s'

const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

export interface LivePlan {
  shape: StatementShape
  sourceName: string
  probeColumns: LiveProbeColumn[]
  keyColumns: string[]
  usesRowid: boolean
  sortPlan: SortKeyPlan
  sortColumns: string[]
  keyColumnAliases: string[]
  readSql: string
  probeSql(probeTable: string): string
  probeParams(params: Params | undefined): unknown[]
  limit: number | null
  offset: number
}

export interface TableShape {
  columns: { name: string; type: string }[]
  collations: Map<string, string>
  pkColumns: string[]
}

export function buildLivePlan(shape: StatementShape, table: TableShape, params: Params | undefined): LivePlan {
  const sourceName = shape.alias ?? shape.table
  assertIdentifier(shape, sourceName, 'the table or its alias')
  for (const column of table.columns) {
    if (column.name.toLowerCase().startsWith(INTERNAL_TABLE_PREFIX)) {
      throw new CDCError(
        `Cannot open a live query for '${shape.sql}': column '${column.name}' uses the '${INTERNAL_TABLE_PREFIX}' prefix Sirannon reserves`,
      )
    }
  }

  const usesRowid = table.pkColumns.length === 0
  const keyColumns = usesRowid ? ['rowid'] : table.pkColumns
  const keyColumnAliases = keyColumns.map((_, index) => `${KEY_COLUMN}${index}`)
  const sortColumns = shape.sortTerms.map((_, index) => `${SORT_COLUMN_PREFIX}${index}`)

  const sortExpressions = shape.sortTerms.map(term => resolveSortExpression(shape, term))
  const collations = shape.sortTerms.map((term, index) =>
    effectiveCollation(term, sortExpressions[index], sourceName, table.collations),
  )
  const sortPlan = buildSortKeyPlan(shape.sortTerms.map((term, index) => ({ ...term, collation: collations[index] })))

  const projection = [
    shape.selectList,
    ...sortExpressions.map((expression, index) => `${expression} AS "${sortColumns[index]}"`),
  ].join('\n, ')

  const keyProjection = keyColumns
    .map((column, index) => `"${sourceName}"."${column}" AS "${keyColumnAliases[index]}"`)
    .join(', ')

  const from = shape.alias === null ? `"${shape.table}"` : `"${shape.table}" AS "${shape.alias}"`
  const where = shape.where === null ? '' : `\nWHERE (${shape.where}\n)`
  const tail = shape.tail.length === 0 ? '' : `\n${shape.tail}`

  return {
    shape,
    sourceName,
    probeColumns: table.columns.map(column => ({
      name: column.name,
      type: column.type,
      collation: table.collations.get(column.name) ?? null,
    })),
    keyColumns,
    usesRowid,
    sortPlan,
    sortColumns,
    keyColumnAliases,
    readSql: `SELECT ${projection}\n, ${keyProjection}\nFROM ${from}${where}${tail}`,
    probeSql: probeTable => selectLiveProbeMatchesSql(probeTable, KEY_COLUMN, projection, shape.where, sourceName),
    probeParams: bound => sliceProbeParams(shape, bound),
    limit: shape.limit === null ? null : resolveBound(shape, shape.limit, params, 'LIMIT'),
    offset: shape.offset === null ? 0 : resolveBound(shape, shape.offset, params, 'OFFSET'),
  }
}

function sliceProbeParams(shape: StatementShape, params: Params | undefined): unknown[] {
  if (params === undefined) return []
  if (!Array.isArray(params)) return [params]
  return params.slice(0, shape.selectParameters + shape.whereParameters)
}

export function probeNamedParams(probeSql: string, params: Params | undefined): Params | undefined {
  if (params === undefined || Array.isArray(params)) return params
  const used = new Set<string>()
  for (const token of tokenizeSql(probeSql)) {
    const name = parameterName(token)
    if (name !== null) used.add(name)
  }
  const picked: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(params)) {
    if (used.has(key)) picked[key] = value
  }
  return picked
}

function resolveSortExpression(shape: StatementShape, term: SortTerm): string {
  const tokens = tokenizeSql(term.expression)
  if (tokens.length === 1 && tokens[0].kind === 'number') {
    return resolveOrdinal(shape, Number(tokens[0].value))
  }
  if (tokens.length === 1 && tokens[0].kind === 'word') {
    const item = shape.selectItems.find(candidate => candidate.alias === tokens[0].value)
    if (item !== undefined) return item.expression
  }
  return term.expression
}

function resolveOrdinal(shape: StatementShape, ordinal: number): string {
  if (shape.selectItems.some(item => item.star)) {
    throw new CDCError(
      `Cannot open a live query for '${shape.sql}': ORDER BY ${ordinal} cannot be resolved through '*', so name the column instead`,
    )
  }
  const item: SelectItem | undefined = shape.selectItems[ordinal - 1]
  if (item === undefined) {
    throw new CDCError(
      `Cannot open a live query for '${shape.sql}': ORDER BY ${ordinal} names no column of the select list`,
    )
  }
  return item.expression
}

function effectiveCollation(
  term: SortTerm,
  expression: string,
  sourceName: string,
  collations: Map<string, string>,
): string | null {
  if (term.collation !== null) return term.collation
  const column = bareColumnReference(expression, sourceName)
  return column === null ? null : (collations.get(column) ?? null)
}

function bareColumnReference(expression: string, sourceName: string): string | null {
  const tokens = tokenizeSql(expression)
  if (tokens.length === 1 && tokens[0].kind === 'word') return tokens[0].value
  if (tokens.length === 3 && tokens[0].kind === 'word' && tokens[1].value === '.' && tokens[2].kind === 'word') {
    return tokens[0].value === sourceName ? tokens[2].value : null
  }
  return null
}

function resolveBound(shape: StatementShape, bound: WindowBound, params: Params | undefined, label: string): number {
  if (bound.kind === 'literal') return bound.literal

  const value = boundParameterValue(bound, params, shape)
  if (typeof value === 'bigint') return assertWholeBound(shape, Number(value), label)
  if (typeof value === 'number') return assertWholeBound(shape, value, label)
  throw new CDCError(`Cannot open a live query for '${shape.sql}': ${label} is bound to a value that is not a number`)
}

function boundParameterValue(bound: WindowBound, params: Params | undefined, shape: StatementShape): unknown {
  if (params === undefined) {
    throw new CDCError(`Cannot open a live query for '${shape.sql}': ${bound.parameter} has no bound value`)
  }
  if (!Array.isArray(params)) return (params as Record<string, unknown>)[bound.parameter.slice(1)]

  const numbered = bound.parameter.length > 1 ? Number(bound.parameter.slice(1)) : 0
  const ordinal = numbered > 0 ? numbered : placeholderOrdinal(shape.sql, bound.position)
  return params[ordinal - 1]
}

function placeholderOrdinal(sql: string, position: number): number {
  let ordinal = 0
  for (const token of tokenizeSql(sql)) {
    if (token.kind !== 'param') continue
    ordinal++
    if (token.start === position) return ordinal
  }
  return ordinal
}

function assertWholeBound(shape: StatementShape, value: number, label: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new CDCError(
      `Cannot open a live query for '${shape.sql}': ${label} is bound to ${value}, and it takes a non-negative whole number`,
    )
  }
  return value
}

function assertIdentifier(shape: StatementShape, name: string, label: string): void {
  if (!IDENTIFIER_RE.test(name)) {
    throw new CDCError(
      `Cannot open a live query for '${shape.sql}': ${label} must contain only letters, digits, and underscores, and start with a letter or underscore`,
    )
  }
}
