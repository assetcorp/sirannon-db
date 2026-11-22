import type { Counted, SQLiteStatement } from '../driver/types.js'
import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'

export function statementCallsAround(
  stmt: SQLiteStatement,
  around: <T>(call: () => Promise<T>) => Promise<T>,
): SQLiteStatement {
  const { allRaw, allCounted, getCounted, allRawCounted } = stmt
  const wrapped: SQLiteStatement = {
    ...stmt,
    run: (...params) => around(() => stmt.run(...params)),
    all: <T>(...params: unknown[]) => around(() => stmt.all<T>(...params)),
    get: <T>(...params: unknown[]) => around(() => stmt.get<T>(...params)),
  }
  if (allRaw) {
    wrapped.allRaw = <T>(...params: unknown[]) =>
      around(() => allRaw.call<SQLiteStatement, unknown[], Promise<T[]>>(stmt, ...params))
  }
  if (allCounted) {
    wrapped.allCounted = <T>(...params: unknown[]) =>
      around(() => allCounted.call<SQLiteStatement, unknown[], Promise<Counted<T[]>>>(stmt, ...params))
  }
  if (getCounted) {
    wrapped.getCounted = <T>(...params: unknown[]) =>
      around(() => getCounted.call<SQLiteStatement, unknown[], Promise<Counted<T | undefined>>>(stmt, ...params))
  }
  if (allRawCounted) {
    wrapped.allRawCounted = <T>(...params: unknown[]) =>
      around(() => allRawCounted.call<SQLiteStatement, unknown[], Promise<Counted<T[]>>>(stmt, ...params))
  }
  return wrapped
}

export function splitMigrationScript(sql: string): Array<{ text: string; tokens: SqlToken[] }> {
  const statements: Array<{ text: string; tokens: SqlToken[] }> = []
  let tokens: SqlToken[] = []
  let start = 0

  for (const token of tokenizeSql(sql)) {
    if (token.kind === 'punct' && token.value === ';' && token.depth === 0) {
      const next = isKeyword(tokens[1], 'temp') || isKeyword(tokens[1], 'temporary') ? tokens[2] : tokens[1]
      const trigger = isKeyword(tokens[0], 'create') && isKeyword(next, 'trigger')
      const closed = isKeyword(tokens.at(-1), 'end') && tokens.at(-2)?.value === ';'
      if (!trigger || closed) {
        if (tokens.length > 0) statements.push({ text: sql.slice(start, token.end), tokens })
        tokens = []
        start = token.end
        continue
      }
    }
    tokens.push(token)
  }
  if (tokens.length > 0) statements.push({ text: sql.slice(start), tokens })
  return statements
}
