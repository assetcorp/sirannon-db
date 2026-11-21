import type { Counted, SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import { LEADING_COMMENTS_PATTERN } from '../sql-statement-start.js'
import type { ChangeTracker } from './change-tracker.js'
import { dropCdcTriggers } from './trigger-sql.js'

const ALTER_TABLE_START_RE = new RegExp(`${LEADING_COMMENTS_PATTERN}ALTER\\s+TABLE\\b`, 'i')
const DROP_WORD_RE = /\bDROP\b/i
const COLUMN_DROP_SAVEPOINT = 'sirannon_column_drop'

/**
 * One statement of a script, with its text and its tokens.
 *
 * @internal
 */
export interface ScriptStatement {
  /** The statement's text, from the end of the statement before it up to and including its closing semicolon. */
  text: string
  /** The statement's tokens, without whitespace and comments. */
  tokens: SqlToken[]
}

/**
 * Returns what one statement returns, and lets SQLite drop a column of a watched table: before such a statement,
 * Sirannon drops the table's change-capture triggers, and after it, Sirannon installs them again for the new column
 * list, all inside one savepoint.
 *
 * SQLite refuses `ALTER TABLE ... DROP COLUMN` while a trigger names the column, so the triggers have to be absent
 * while SQLite executes the statement. When the statement fails, the savepoint rolls back and the old triggers stay.
 *
 * @param tracker - The change tracker of the database, or `null` when it watches nothing.
 * @param conn - The writer connection for the statement, inside or outside a transaction.
 * @param sql - The statement.
 * @param execute - Executes the statement on `conn`.
 * @returns What `execute` returns.
 */
export function executeAllowingColumnDrop<T>(
  tracker: ChangeTracker | null,
  conn: SQLiteConnection,
  sql: string,
  execute: () => Promise<T>,
): Promise<T> {
  if (tracker === null || tracker.watchedTables.size === 0 || !ALTER_TABLE_START_RE.test(sql)) return execute()
  const table = columnDropTarget(tokenizeSql(sql), tracker)
  if (table === undefined) return execute()
  return withTriggersLifted(tracker, conn, table, execute)
}

/**
 * Executes a script of statements, such as a migration, and treats each statement in it that drops a column of a
 * watched table as {@link executeAllowingColumnDrop} does.
 *
 * Sirannon passes a script without such a statement to SQLite in one call, unchanged. It executes a script with one
 * statement by statement, so that every other write in it still reaches the change log.
 *
 * @param tracker - The change tracker of the database, or `null` when it watches nothing.
 * @param conn - The writer connection for the script.
 * @param sql - The script.
 * @returns A promise that settles once SQLite has executed the whole script.
 */
export async function execAllowingColumnDrops(
  tracker: ChangeTracker | null,
  conn: SQLiteConnection,
  sql: string,
): Promise<void> {
  if (tracker === null || tracker.watchedTables.size === 0 || !DROP_WORD_RE.test(sql)) return conn.exec(sql)
  const statements = splitScript(sql)
  if (!statements.some(statement => columnDropTarget(statement.tokens, tracker) !== undefined)) return conn.exec(sql)

  for (const statement of statements) {
    const table = columnDropTarget(statement.tokens, tracker)
    if (table === undefined) await conn.exec(statement.text)
    else await withTriggersLifted(tracker, conn, table, () => conn.exec(statement.text))
  }
}

/**
 * Returns a view of a connection that treats every statement and script dropping a column of a watched table as
 * {@link executeAllowingColumnDrop} and {@link execAllowingColumnDrops} do, for a caller such as the migration runner,
 * which executes whatever schema changes a migration holds.
 *
 * The view keeps every member of `conn` and of each statement that it prepares, so that a batch, a grouped write, a
 * stepped copy, an extension load, and a counted read all still reach the driver.
 *
 * @param tracker - The change tracker of the database.
 * @param conn - The writer connection to wrap.
 * @returns A connection whose statements, scripts, and transactions all pass through `conn`.
 */
export function columnDropAwareConnection(tracker: ChangeTracker, conn: SQLiteConnection): SQLiteConnection {
  const { runBatch, runBatchSummary } = conn
  const wrapped: SQLiteConnection = {
    ...conn,
    exec: sql => execAllowingColumnDrops(tracker, conn, sql),
    prepare: async sql => {
      const stmt = await conn.prepare(sql)
      return ALTER_TABLE_START_RE.test(sql) ? columnDropAwareStatement(tracker, conn, sql, stmt) : stmt
    },
    transaction: fn => conn.transaction(txConn => fn(columnDropAwareConnection(tracker, txConn))),
  }
  if (runBatch) {
    wrapped.runBatch = (sql, paramsBatch) =>
      executeAllowingColumnDrop(tracker, conn, sql, () => runBatch.call(conn, sql, paramsBatch))
  }
  if (runBatchSummary) {
    wrapped.runBatchSummary = (sql, paramsBatch) =>
      executeAllowingColumnDrop(tracker, conn, sql, () => runBatchSummary.call(conn, sql, paramsBatch))
  }
  return wrapped
}

function columnDropAwareStatement(
  tracker: ChangeTracker,
  conn: SQLiteConnection,
  sql: string,
  stmt: SQLiteStatement,
): SQLiteStatement {
  return statementCallsAround(stmt, call => executeAllowingColumnDrop(tracker, conn, sql, call))
}

/**
 * Returns a view of a prepared statement whose every executing call, `run`, `all`, `get`, and each counted or raw
 * variant that the statement has, goes through `around`.
 *
 * @param stmt - The prepared statement to wrap.
 * @param around - Executes the call that it receives and returns what that call returns.
 * @returns A statement that keeps every other member of `stmt`.
 * @internal
 */
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

async function withTriggersLifted<T>(
  tracker: ChangeTracker,
  conn: SQLiteConnection,
  table: string,
  run: () => Promise<T>,
): Promise<T> {
  await conn.exec(`SAVEPOINT ${COLUMN_DROP_SAVEPOINT}`)
  try {
    await dropCdcTriggers(conn, table)
    const result = await run()
    await tracker.refreshAllTriggersUsingConnection(conn, new Set([table]))
    await conn.exec(`RELEASE ${COLUMN_DROP_SAVEPOINT}`)
    return result
  } catch (err) {
    try {
      await conn.exec(`ROLLBACK TO ${COLUMN_DROP_SAVEPOINT}`)
      await conn.exec(`RELEASE ${COLUMN_DROP_SAVEPOINT}`)
    } catch {}
    throw err
  }
}

function columnDropTarget(tokens: readonly SqlToken[], tracker: ChangeTracker): string | undefined {
  if (!isKeyword(tokens[0], 'alter') || !isKeyword(tokens[1], 'table')) return undefined
  const qualified = tokens[3]?.kind === 'punct' && tokens[3].value === '.'
  if (qualified && tokens[2]?.lower !== 'main') return undefined
  const nameAt = qualified ? 4 : 2
  const name = tokens[nameAt]
  if (name === undefined || name.kind !== 'word' || !isKeyword(tokens[nameAt + 1], 'drop')) return undefined
  return tracker.watchedTableNamed(name.value)
}

/**
 * Returns the statements of a script, in order, keeping the body of a `CREATE TRIGGER` in one statement.
 *
 * @param sql - The script.
 * @returns One entry per statement, each with its text and its tokens.
 * @internal
 */
export function splitScript(sql: string): ScriptStatement[] {
  const statements: ScriptStatement[] = []
  let tokens: SqlToken[] = []
  let start = 0
  let caseDepth = 0
  let triggerBodyClosed = false

  for (const token of tokenizeSql(sql)) {
    if (token.kind === 'punct' && token.value === ';' && token.depth === 0) {
      if (!createsTrigger(tokens) || triggerBodyClosed) {
        if (tokens.length > 0) statements.push({ text: sql.slice(start, token.end), tokens })
        tokens = []
        start = token.end
        caseDepth = 0
        triggerBodyClosed = false
        continue
      }
    }
    tokens.push(token)
    if (token.kind !== 'word' || token.quoted) continue
    if (token.lower === 'case') caseDepth++
    else if (token.lower === 'end') {
      if (caseDepth > 0) caseDepth--
      else triggerBodyClosed = true
    }
  }
  if (tokens.length > 0) statements.push({ text: sql.slice(start), tokens })
  return statements
}

function createsTrigger(tokens: readonly SqlToken[]): boolean {
  if (!isKeyword(tokens[0], 'create')) return false
  const next = isKeyword(tokens[1], 'temp') || isKeyword(tokens[1], 'temporary') ? tokens[2] : tokens[1]
  return isKeyword(next, 'trigger')
}
