import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import { escId } from '../sql-identifiers.js'

const EXPRESSION_KEYWORDS = new Set([
  'and',
  'as',
  'between',
  'case',
  'cast',
  'collate',
  'current_date',
  'current_time',
  'current_timestamp',
  'distinct',
  'else',
  'end',
  'escape',
  'exists',
  'false',
  'glob',
  'in',
  'is',
  'isnull',
  'like',
  'match',
  'not',
  'notnull',
  'null',
  'or',
  'regexp',
  'then',
  'true',
  'when',
])

/**
 * Holds the key expressions and the `WHERE` clause of a `CREATE INDEX` statement, each rebuilt from its tokens without comments, ordering, or collation.
 *
 * @internal
 */
export interface IndexDefinition {
  elements: string[]
  where: string | null
}

/**
 * Holds an expression whose column references point at one row, with the columns that it reads.
 *
 * @internal
 */
export interface RewrittenExpression {
  sql: string
  columns: Set<string>
}

/**
 * Reads the key expressions and the `WHERE` clause of the statement that SQLite stores for an index.
 *
 * @param sql - The `CREATE INDEX` statement from `sqlite_master`.
 * @returns The definition, or `null` when the statement holds no parenthesised key list.
 * @internal
 */
export function parseIndexDefinition(sql: string): IndexDefinition | null {
  const tokens = tokenizeSql(sql)
  const on = tokens.findIndex(token => isKeyword(token, 'on') && token.depth === 0)
  const open = tokens.findIndex((token, i) => i > on && token.kind === 'punct' && token.value === '(')
  if (on === -1 || open === -1) return null
  const close = tokens.findIndex(
    (token, i) => i > open && token.kind === 'punct' && token.value === ')' && token.depth === 0,
  )
  if (close === -1) return null

  const elements: string[] = []
  let start = open + 1
  for (let i = open + 1; i <= close; i++) {
    const token = tokens[i]
    if (i < close && !(token.kind === 'punct' && token.value === ',' && token.depth === 1)) continue
    const element = withoutOrderAndCollation(tokens.slice(start, i))
    if (element.length === 0) return null
    elements.push(rebuild(sql, element))
    start = i + 1
  }

  const whereAt = tokens.findIndex((token, i) => i > close && isKeyword(token, 'where') && token.depth === 0)
  const condition = whereAt === -1 ? [] : tokens.slice(whereAt + 1).filter(token => token.value !== ';')
  return { elements, where: condition.length === 0 ? null : rebuild(sql, condition) }
}

/**
 * Rewrites every column reference in an expression of an index so that it reads that column from the given row, such as `NEW`.
 *
 * @param expression - An expression from {@link parseIndexDefinition}.
 * @param columns - Every column of the indexed table, generated columns included.
 * @param row - The row reference to read each column from, such as `NEW`, or a function that returns the SQL to put in place of each column.
 * @returns The rewritten expression and the stored names of the columns that it reads.
 * @internal
 */
export function rewriteColumnReferences(
  expression: string,
  columns: readonly string[],
  row: string | ((column: string) => string),
): RewrittenExpression {
  const replacement = typeof row === 'string' ? (column: string) => `${row}."${escId(column)}"` : row
  const byFoldedName = new Map(columns.map(column => [column.toLowerCase(), column]))
  const tokens = tokenizeSql(expression)
  const read = new Set<string>()
  const replaced = new Map<number, string>()
  const dropped = new Set<number>()
  let typeNameAfter = -1
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.kind !== 'word') {
      if (token.value !== '(' || typeNameAfter === -1) typeNameAfter = -1
      continue
    }
    if (typeNameAfter !== -1) continue
    const previous = tokens[i - 1]
    if (previous !== undefined && (isKeyword(previous, 'collate') || isPunct(previous, '.'))) continue
    if (isKeyword(token, 'as')) {
      typeNameAfter = i
      continue
    }
    if (isPunct(tokens[i + 1], '(')) continue
    const qualified = isPunct(tokens[i + 1], '.') && tokens[i + 2]?.kind === 'word'
    const at = qualified ? i + 2 : i
    const reference = tokens[at]
    if (!qualified && !token.quoted && EXPRESSION_KEYWORDS.has(token.lower)) continue
    const column = byFoldedName.get(reference.lower)
    if (column === undefined) continue
    if (qualified) {
      dropped.add(i)
      dropped.add(i + 1)
      i += 2
    }
    read.add(column)
    replaced.set(at, replacement(column))
  }
  return { sql: rebuild(expression, tokens, replaced, dropped), columns: read }
}

function withoutOrderAndCollation(tokens: SqlToken[]): SqlToken[] {
  let end = tokens.length
  const last = tokens[end - 1]
  if (last !== undefined && (isKeyword(last, 'asc') || isKeyword(last, 'desc'))) end--
  const collate = tokens[end - 2]
  if (collate !== undefined && isKeyword(collate, 'collate') && collate.depth === tokens[0]?.depth) end -= 2
  return tokens.slice(0, end)
}

function rebuild(
  source: string,
  tokens: readonly SqlToken[],
  replaced: ReadonlyMap<number, string> = new Map(),
  dropped: ReadonlySet<number> = new Set(),
): string {
  let sql = ''
  let previousEnd = -1
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (previousEnd !== -1 && source.slice(previousEnd, token.start).length > 0 && sql.length > 0) sql += ' '
    previousEnd = token.end
    if (dropped.has(i)) continue
    sql += replaced.get(i) ?? (token.kind === 'word' && token.quoted ? `"${escId(token.value)}"` : token.value)
  }
  return sql
}

function isPunct(token: SqlToken | undefined, value: string): boolean {
  return token !== undefined && token.kind === 'punct' && token.value === value
}
