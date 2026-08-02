import { CDCError } from '../errors.js'
import type { SqlToken } from './sql-tokens.js'

export type SortDirection = 'asc' | 'desc'
export type NullPlacement = 'first' | 'last'

export interface SelectItem {
  expression: string
  alias: string | null
  star: boolean
}

export interface SortTerm {
  expression: string
  direction: SortDirection
  nulls: NullPlacement
  collation: string | null
}

export interface WindowBound {
  kind: 'literal' | 'parameter'
  literal: number
  parameter: string
  position: number
}

export interface ClausePositions {
  selectStart: number
  fromKeyword: number
  fromStart: number
  fromEnd: number
  whereStart: number | null
  whereEnd: number | null
  orderStart: number | null
  orderEnd: number | null
  limitStart: number | null
  tailStart: number
  statementEnd: number
}

export function unsupported(sql: string, reason: string): CDCError {
  return new CDCError(`Cannot open a live query for '${sql}': ${reason}`)
}

export function findClauses(sql: string, body: SqlToken[]): ClausePositions {
  let fromIndex = -1
  let whereIndex = -1
  let orderIndex = -1
  let limitIndex = -1

  for (let i = 1; i < body.length; i++) {
    const token = body[i]
    if (token.depth !== 0 || token.kind !== 'word' || token.quoted) continue
    if (token.lower === 'from' && fromIndex === -1) fromIndex = i
    else if (token.lower === 'where' && whereIndex === -1) whereIndex = i
    else if (token.lower === 'order' && body[i + 1]?.lower === 'by' && orderIndex === -1) orderIndex = i
    else if (token.lower === 'limit' && limitIndex === -1) limitIndex = i
  }

  if (fromIndex === -1) throw unsupported(sql, 'a live query reads from a table')

  const boundaryAfter = (index: number): number => {
    for (const candidate of [whereIndex, orderIndex, limitIndex]) {
      if (candidate > index) return body[candidate].start
    }
    return body[body.length - 1].end
  }

  const statementEnd = body[body.length - 1].end
  const tailIndex = orderIndex !== -1 ? orderIndex : limitIndex

  return {
    tailStart: tailIndex === -1 ? statementEnd : body[tailIndex].start,
    statementEnd,
    selectStart: body[1] === undefined ? body[0].end : body[1].start,
    fromKeyword: body[fromIndex].start,
    fromStart: body[fromIndex].end,
    fromEnd: boundaryAfter(fromIndex),
    whereStart: whereIndex === -1 ? null : body[whereIndex].end,
    whereEnd: whereIndex === -1 ? null : boundaryAfter(whereIndex),
    orderStart: orderIndex === -1 ? null : body[orderIndex + 1].end,
    orderEnd: orderIndex === -1 ? null : boundaryAfter(orderIndex),
    limitStart: limitIndex === -1 ? null : limitIndex,
  }
}

export function readSource(
  sql: string,
  body: SqlToken[],
  clauses: ClausePositions,
): { table: string; alias: string | null } {
  const source = body.filter(token => token.start >= clauses.fromStart && token.end <= clauses.fromEnd)
  if (source.length === 0 || source[0].kind !== 'word') {
    throw unsupported(sql, 'a live query names one table after FROM')
  }
  if (source.some(token => token.kind === 'punct' && (token.value === ',' || token.value === '('))) {
    throw unsupported(sql, 'a live query reads one table, so a join or a subquery in FROM is not maintainable')
  }

  const table = source[0].value
  const rest = source.slice(1)
  if (rest.length === 0) return { table, alias: null }

  const labelled = rest[0].lower === 'as' && !rest[0].quoted
  const aliasToken = labelled ? rest[1] : rest[0]
  if (rest.length > (labelled ? 2 : 1) || aliasToken === undefined || aliasToken.kind !== 'word') {
    throw unsupported(sql, 'a live query names one table after FROM, with an optional alias')
  }
  return { table, alias: aliasToken.value }
}

export function readSelectItems(sql: string, body: SqlToken[], clauses: ClausePositions): SelectItem[] {
  const spans = splitTopLevel(body, clauses.selectStart, clauses.fromKeyword)
  return spans.map(span => readSelectItem(sql, span))
}

function readSelectItem(sql: string, span: SqlToken[]): SelectItem {
  const last = span[span.length - 1]
  const star = span.some(token => token.kind === 'punct' && token.value === '*')

  if (span.length >= 3 && span[span.length - 2].lower === 'as' && last.kind === 'word') {
    return { expression: sql.slice(span[0].start, span[span.length - 2].start).trim(), alias: last.value, star }
  }
  if (span.length === 1 && last.kind === 'word') {
    return { expression: sql.slice(span[0].start, last.end), alias: last.value, star: false }
  }
  if (span.length === 3 && span[1].value === '.' && last.kind === 'word' && !star) {
    return { expression: sql.slice(span[0].start, last.end), alias: last.value, star: false }
  }
  return { expression: sql.slice(span[0].start, last.end), alias: null, star }
}

export function readSortTerms(sql: string, body: SqlToken[], clauses: ClausePositions): SortTerm[] {
  const start = clauses.orderStart
  const end = clauses.orderEnd
  if (start === null || end === null) return []

  return splitTopLevel(body, start, end).map(span => readSortTerm(sql, span))
}

function readSortTerm(sql: string, span: SqlToken[]): SortTerm {
  let end = span.length
  let nulls: NullPlacement | null = null
  let direction: SortDirection = 'asc'

  if (end >= 2 && span[end - 2].lower === 'nulls') {
    const placement = span[end - 1].lower
    if (placement !== 'first' && placement !== 'last') {
      throw unsupported(sql, 'NULLS in an ORDER BY term is followed by FIRST or LAST')
    }
    nulls = placement
    end -= 2
  }

  if (end >= 1 && (span[end - 1].lower === 'asc' || span[end - 1].lower === 'desc')) {
    direction = span[end - 1].lower === 'desc' ? 'desc' : 'asc'
    end -= 1
  }

  if (end === 0) throw unsupported(sql, 'an ORDER BY term needs an expression')

  const collation = end >= 2 && span[end - 2].lower === 'collate' ? span[end - 1].value : null

  return {
    expression: sql.slice(span[0].start, span[end - 1].end),
    direction,
    nulls: nulls ?? (direction === 'asc' ? 'first' : 'last'),
    collation,
  }
}

export function readWindow(
  sql: string,
  body: SqlToken[],
  clauses: ClausePositions,
): { limit: WindowBound | null; offset: WindowBound | null } {
  if (clauses.limitStart === null) return { limit: null, offset: null }

  const tail = body.slice(clauses.limitStart + 1)
  const separator = tail.findIndex(token => token.depth === 0 && (token.value === ',' || token.lower === 'offset'))
  if (separator === -1) return { limit: readBound(sql, tail), offset: null }

  const first = readBound(sql, tail.slice(0, separator))
  const second = readBound(sql, tail.slice(separator + 1))
  if (tail[separator].value === ',') return { limit: second, offset: first }
  return { limit: first, offset: second }
}

function readBound(sql: string, span: SqlToken[]): WindowBound {
  const token = span.length === 1 ? span[0] : undefined
  if (token?.kind === 'number') {
    const literal = Number(token.value)
    if (!Number.isInteger(literal) || literal < 0) {
      throw unsupported(sql, 'LIMIT and OFFSET in a live query take a non-negative whole number')
    }
    return { kind: 'literal', literal, parameter: '', position: token.start }
  }
  if (token?.kind === 'param') {
    return { kind: 'parameter', literal: 0, parameter: token.value, position: token.start }
  }
  throw unsupported(sql, 'LIMIT and OFFSET in a live query take a number or a bound parameter')
}

function splitTopLevel(body: SqlToken[], start: number, end: number): SqlToken[][] {
  const spans: SqlToken[][] = [[]]
  for (const token of body) {
    if (token.start < start || token.end > end) continue
    if (token.depth === 0 && token.kind === 'punct' && token.value === ',') {
      spans.push([])
      continue
    }
    spans[spans.length - 1].push(token)
  }
  return spans.filter(span => span.length > 0)
}
