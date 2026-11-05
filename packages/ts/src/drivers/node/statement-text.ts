import type { DatabaseSync, StatementSync } from 'node:sqlite'
import { assertHoldsStatement } from '../../core/driver/statement-text.js'

/**
 * Returns a statement prepared on `db` that reads every integer as a `BigInt`, after refusing text from which SQLite
 * would prepare no statement.
 *
 * node:sqlite in Node.js 22 records such an empty statement on the connection, frees it when the garbage collector
 * reclaims it, and then finalizes the freed memory when the connection closes, which crashes the process. Refusing the
 * text before `prepare` keeps that empty statement from ever existing.
 *
 * @param db - The open node:sqlite connection.
 * @param sql - The statement text that a caller supplied.
 * @returns The prepared statement.
 * @throws RangeError when the text holds no statement, and the error that SQLite raises when it cannot prepare the text.
 * @internal
 */
export function prepareStatement(db: DatabaseSync, sql: string): StatementSync {
  assertHoldsStatement(sql)
  const statement = db.prepare(sql)
  statement.setReadBigInts(true)
  return statement
}
