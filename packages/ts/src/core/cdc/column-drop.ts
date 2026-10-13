import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import type { ChangeTracker } from './change-tracker.js'
import { dropCdcTriggers } from './trigger-sql.js'

const ALTER_TABLE_START_RE = /^(?:\s|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*ALTER\s+TABLE\b/i
const DROP_WORD_RE = /\bDROP\b/i
const COLUMN_DROP_SAVEPOINT = 'sirannon_column_drop'

interface ScriptStatement {
  text: string
  tokens: SqlToken[]
}

/**
 * Runs one statement and, when it drops a column of a watched table, drops that table's change-capture triggers first and installs them again for the new column list afterwards, all inside one savepoint.
 *
 * SQLite refuses `ALTER TABLE ... DROP COLUMN` while a trigger names the column, so the triggers have to be absent while the statement runs. When the statement fails, the savepoint rolls back and the old triggers remain.
 *
 * @param tracker - The change tracker of the database, or `null` when it watches nothing.
 * @param conn - The writer connection that runs the statement, inside or outside a transaction.
 * @param sql - The statement.
 * @param run - Runs the statement on `conn`.
 * @returns What `run` returns.
 */
export function runWithColumnDropsPrepared<T>(
  tracker: ChangeTracker | null,
  conn: SQLiteConnection,
  sql: string,
  run: () => Promise<T>,
): Promise<T> {
  if (tracker === null || tracker.watchedTables.size === 0 || !ALTER_TABLE_START_RE.test(sql)) return run()
  const table = columnDropTarget(tokenizeSql(sql), tracker)
  if (table === undefined) return run()
  return withTriggersLifted(tracker, conn, table, run)
}

/**
 * Executes a script of statements, such as a migration, and treats each statement in it that drops a column of a watched table as {@link runWithColumnDropsPrepared} does.
 *
 * A script without such a statement runs in one call, unchanged. A script with one runs statement by statement, so that every other write in it still reaches the change log.
 *
 * @param tracker - The change tracker of the database, or `null` when it watches nothing.
 * @param conn - The writer connection that runs the script.
 * @param sql - The script.
 */
export async function execWithColumnDropsPrepared(
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
 * Returns a view of a connection that treats every statement and script dropping a column of a watched table as {@link runWithColumnDropsPrepared} and {@link execWithColumnDropsPrepared} do, for a caller such as the migration runner that runs arbitrary schema changes.
 *
 * @param tracker - The change tracker of the database.
 * @param conn - The writer connection to wrap.
 * @returns A connection whose statements, scripts, and transactions all pass through `conn`.
 */
export function columnDropAwareConnection(tracker: ChangeTracker, conn: SQLiteConnection): SQLiteConnection {
  return {
    exec: sql => execWithColumnDropsPrepared(tracker, conn, sql),
    prepare: async sql => {
      const stmt = await conn.prepare(sql)
      return ALTER_TABLE_START_RE.test(sql) ? columnDropAwareStatement(tracker, conn, sql, stmt) : stmt
    },
    transaction: fn => conn.transaction(txConn => fn(columnDropAwareConnection(tracker, txConn))),
    close: () => conn.close(),
  }
}

function columnDropAwareStatement(
  tracker: ChangeTracker,
  conn: SQLiteConnection,
  sql: string,
  stmt: SQLiteStatement,
): SQLiteStatement {
  return {
    all<T>(...params: unknown[]) {
      return stmt.all<T>(...params)
    },
    get<T>(...params: unknown[]) {
      return stmt.get<T>(...params)
    },
    run(...params: unknown[]) {
      return runWithColumnDropsPrepared(tracker, conn, sql, () => stmt.run(...params))
    },
  }
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

function isKeyword(token: SqlToken | undefined, word: string): boolean {
  return token !== undefined && token.kind === 'word' && !token.quoted && token.lower === word
}

function splitScript(sql: string): ScriptStatement[] {
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
