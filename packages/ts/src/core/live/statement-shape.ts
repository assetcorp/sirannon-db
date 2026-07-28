import { assertSqlAllowed } from '../internal-tables.js'
import { type SqlToken, tokenizeSql } from './sql-tokens.js'
import type { ClausePositions, SelectItem, SortTerm, WindowBound } from './statement-clauses.js'
import {
  findClauses,
  readSelectItems,
  readSortTerms,
  readSource,
  readWindow,
  unsupported,
} from './statement-clauses.js'

export type { NullPlacement, SelectItem, SortDirection, SortTerm, WindowBound } from './statement-clauses.js'

export interface StatementShape {
  sql: string
  table: string
  alias: string | null
  selectList: string
  selectItems: SelectItem[]
  where: string | null
  sortTerms: SortTerm[]
  limit: WindowBound | null
  offset: WindowBound | null
  tail: string
  selectParameters: number
  whereParameters: number
  usesPositionalParameters: boolean
}

const UNSUPPORTED_WORDS = new Map<string, string>([
  ['union', 'a compound SELECT'],
  ['intersect', 'a compound SELECT'],
  ['except', 'a compound SELECT'],
  ['group', 'GROUP BY'],
  ['having', 'HAVING'],
  ['window', 'a window clause'],
  ['join', 'a join'],
  ['natural', 'a join'],
  ['cross', 'a join'],
  ['inner', 'a join'],
  ['outer', 'a join'],
  ['left', 'a join'],
  ['right', 'a join'],
  ['full', 'a join'],
  ['with', 'a common table expression'],
  ['values', 'a VALUES clause'],
  ['distinct', 'DISTINCT'],
])

const AGGREGATE_FUNCTIONS = new Set([
  'avg',
  'count',
  'group_concat',
  'json_group_array',
  'json_group_object',
  'max',
  'min',
  'string_agg',
  'sum',
  'total',
])

export function analyseStatement(sql: string): StatementShape {
  assertSqlAllowed(sql)
  const tokens = tokenizeSql(sql)
  if (tokens.length === 0 || tokens[0].lower !== 'select' || tokens[0].quoted) {
    throw unsupported(sql, 'a live query reads with a single SELECT statement')
  }

  const body = withoutTrailingSemicolon(tokens)
  assertSupportedWords(sql, body)

  const clauses = findClauses(sql, body)
  const source = readSource(sql, body, clauses)
  const sortTerms = readSortTerms(sql, body, clauses)
  const window = readWindow(sql, body, clauses)

  if (window.limit !== null && sortTerms.length === 0) {
    throw unsupported(sql, 'a live query with LIMIT orders its rows, otherwise the window has no defined membership')
  }

  const usesPositionalParameters = body.some(token => token.kind === 'param' && token.value.startsWith('?'))
  assertSortTermsBindByName(sql, sortTerms, usesPositionalParameters)

  return {
    sql,
    table: source.table,
    alias: source.alias,
    selectList: sql.slice(clauses.selectStart, clauses.fromKeyword).trim(),
    selectItems: readSelectItems(sql, body, clauses),
    where: readWhere(sql, clauses),
    sortTerms,
    limit: window.limit,
    offset: window.offset,
    tail: sql.slice(clauses.tailStart, clauses.statementEnd).trim(),
    selectParameters: countParameters(body, clauses.selectStart, clauses.fromKeyword),
    whereParameters:
      clauses.whereStart === null || clauses.whereEnd === null
        ? 0
        : countParameters(body, clauses.whereStart, clauses.whereEnd),
    usesPositionalParameters,
  }
}

function countParameters(body: SqlToken[], start: number, end: number): number {
  return body.filter(token => token.kind === 'param' && token.start >= start && token.end <= end).length
}

function readWhere(sql: string, clauses: ClausePositions): string | null {
  if (clauses.whereStart === null || clauses.whereEnd === null) return null
  return sql.slice(clauses.whereStart, clauses.whereEnd).trim()
}

function assertSortTermsBindByName(sql: string, sortTerms: SortTerm[], positional: boolean): void {
  if (!positional) return
  for (const term of sortTerms) {
    if (tokenizeSql(term.expression).some(token => token.kind === 'param')) {
      throw unsupported(
        sql,
        'a live query binds an ORDER BY parameter by name, because it evaluates the term a second time to place a changed row',
      )
    }
  }
}

function assertSupportedWords(sql: string, body: SqlToken[]): void {
  for (let i = 0; i < body.length; i++) {
    const token = body[i]
    if (token.kind !== 'word' || token.quoted) continue

    if (i > 0 && token.lower === 'select') {
      throw unsupported(sql, 'a subquery in a live query is not maintainable')
    }
    if (token.lower === 'over' && body[i + 1]?.value === '(') {
      throw unsupported(sql, 'a window function in a live query is not maintainable')
    }
    if (AGGREGATE_FUNCTIONS.has(token.lower) && body[i + 1]?.value === '(') {
      throw unsupported(sql, `the aggregate ${token.value} in a live query is not maintainable`)
    }

    const reason = UNSUPPORTED_WORDS.get(token.lower)
    if (reason !== undefined && token.depth === 0) {
      throw unsupported(sql, `${reason} in a live query is not maintainable`)
    }
  }
  if (body.some(token => token.kind === 'punct' && token.value === ';')) {
    throw unsupported(sql, 'a live query reads with one statement')
  }
}

function withoutTrailingSemicolon(tokens: SqlToken[]): SqlToken[] {
  const last = tokens[tokens.length - 1]
  if (last !== undefined && last.kind === 'punct' && last.value === ';') return tokens.slice(0, -1)
  return tokens
}
