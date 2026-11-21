import type { SQLiteConnection } from '../driver/types.js'
import { MigrationError } from '../errors.js'
import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import { foreignKeysEnabled, foreignKeyViolations, setForeignKeysEnabled } from '../system-catalog/index.js'
import type { Migration } from './types.js'

const FOREIGN_KEYS_WORD_RE = /foreign_keys/i
const OFF_VALUES: ReadonlySet<string> = new Set(['off', 'no', 'false', '0'])
const VIOLATIONS_LISTED = 5

type MigrationStep = Migration['up']

/**
 * Returns whether a step of a migration asks to run with foreign keys off: a string step holding a
 * `PRAGMA foreign_keys = OFF` statement anywhere among its statements, or any step of a migration that sets
 * `withoutForeignKeys`.
 *
 * @param migration - The migration.
 * @param step - The `up` or the `down` of that migration.
 * @returns `true` when the runner has to turn foreign keys off around the step's transaction.
 * @internal
 */
export function runsWithoutForeignKeys(migration: Migration, step: MigrationStep | undefined): boolean {
  if (migration.withoutForeignKeys === true) return true
  return typeof step === 'string' && FOREIGN_KEYS_WORD_RE.test(step) && turnsForeignKeysOff(tokenizeSql(step))
}

function turnsForeignKeysOff(tokens: readonly SqlToken[]): boolean {
  let statementStart = true
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.kind === 'punct' && token.value === ';') {
      statementStart = true
      continue
    }
    if (statementStart && isKeyword(token, 'pragma') && setsForeignKeysOff(tokens, i + 1)) return true
    statementStart = false
  }
  return false
}

function setsForeignKeysOff(tokens: readonly SqlToken[], from: number): boolean {
  let at = from
  if (tokens[at + 1]?.kind === 'punct' && tokens[at + 1]?.value === '.') at += 2
  const name = tokens[at]
  if (name === undefined || name.kind !== 'word' || name.lower !== 'foreign_keys') return false
  const operator = tokens[at + 1]
  if (operator?.kind !== 'punct' || (operator.value !== '=' && operator.value !== '(')) return false
  const value = tokens[at + 2]
  if (value === undefined || (value.kind !== 'word' && value.kind !== 'number' && value.kind !== 'string')) {
    return false
  }
  const text = value.kind === 'string' ? value.value.slice(1, -1) : value.value
  return OFF_VALUES.has(text.toLowerCase())
}

/**
 * Runs one migration transaction with foreign keys off on the writer, as SQLite's procedure for copying a table
 * requires, and turns them back on afterwards whatever the outcome.
 *
 * SQLite changes the setting only outside a transaction, so the function turns foreign keys off before `run` opens
 * its transaction, and `run` checks every foreign key before it commits with {@link assertForeignKeysHold}.
 *
 * @param conn - The writer connection, outside any transaction.
 * @param version - The version of the migration, for the error.
 * @param code - The error code that a failure to turn foreign keys off carries.
 * @param run - Opens, runs, and commits the transaction.
 * @returns What `run` returns.
 * @throws {@link MigrationError} when the connection keeps enforcing foreign keys, because a transaction is open.
 * @internal
 */
export async function withForeignKeysOff<T>(
  conn: SQLiteConnection,
  version: number,
  code: string,
  run: () => Promise<T>,
): Promise<T> {
  if (!(await foreignKeysEnabled(conn))) return run()
  await setForeignKeysEnabled(conn, false)
  let result: T
  try {
    if (await foreignKeysEnabled(conn)) {
      throw new MigrationError(
        `Migration ${version} runs with foreign keys off, and SQLite kept them on because a transaction is open on the writer`,
        version,
        code,
      )
    }
    result = await run()
  } catch (err) {
    await restoreForeignKeys(conn, version, code)
    throw err
  }
  await restoreForeignKeys(conn, version, code)
  return result
}

async function restoreForeignKeys(conn: SQLiteConnection, version: number, code: string): Promise<void> {
  await setForeignKeysEnabled(conn, true)
  if (await foreignKeysEnabled(conn)) return
  throw new MigrationError(
    `Migration ${version} could not turn foreign keys back on, because a transaction is still open on the writer`,
    version,
    code,
  )
}

/**
 * Checks every foreign key in the database inside the migration's transaction, before it commits.
 *
 * @param txConn - The writer connection inside the transaction.
 * @param label - The migration, as `<version>_<name>`, for the error.
 * @param version - The version of the migration.
 * @param code - The error code that the failure carries.
 * @throws {@link MigrationError} that lists the first rows whose foreign key refers to no parent row, so that the
 * transaction rolls back.
 * @internal
 */
export async function assertForeignKeysHold(
  txConn: SQLiteConnection,
  label: string,
  version: number,
  code: string,
): Promise<void> {
  const violations = await foreignKeyViolations(txConn)
  if (violations.length === 0) return
  const listed = violations
    .slice(0, VIOLATIONS_LISTED)
    .map(row => `${row.table} row ${String(row.rowid)} refers to a missing row of ${row.parent}`)
    .join('; ')
  const more = violations.length > VIOLATIONS_LISTED ? `, and ${violations.length - VIOLATIONS_LISTED} more` : ''
  throw new MigrationError(
    `Migration ${label} failed: ${violations.length} rows break a foreign key: ${listed}${more}`,
    version,
    code,
  )
}
