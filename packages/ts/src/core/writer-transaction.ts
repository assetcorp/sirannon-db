import type { SQLiteConnection } from './driver/types.js'
import { SirannonError, TransactionError } from './errors.js'

/**
 * Returns the error that Sirannon raises when SQLite refuses to commit a transaction, with SQLite's own error as the cause.
 *
 * @param err - The error that the failed `COMMIT` raised.
 * @returns A `TransactionError`, or `err` itself when it is already a `SirannonError`.
 * @internal
 */
export function commitFailure(err: unknown): SirannonError {
  if (err instanceof SirannonError) return err
  const reason = err instanceof Error ? err.message : String(err)
  return new TransactionError(`SQLite could not commit the transaction: ${reason}`, err)
}

/**
 * Executes `body` inside one transaction on `conn` and returns its result, raising `TRANSACTION_ERROR` when the body succeeds but SQLite refuses the commit.
 *
 * @param conn - The writer connection that owns the transaction.
 * @param body - The work to execute inside the transaction.
 * @returns What `body` returns.
 * @internal
 */
export async function runWriterTransaction<T>(
  conn: SQLiteConnection,
  body: (txConn: SQLiteConnection) => Promise<T>,
): Promise<T> {
  let bodyFinished = false
  try {
    return await conn.transaction(async txConn => {
      const value = await body(txConn)
      bodyFinished = true
      return value
    })
  } catch (err: unknown) {
    if (!bodyFinished) throw err
    throw commitFailure(err)
  }
}

/**
 * Executes some work inside one transaction on a writer connection and returns what that work returns.
 *
 * @internal
 */
export type WriterTransactionRunner = <T>(
  conn: SQLiteConnection,
  body: (txConn: SQLiteConnection) => Promise<T>,
) => Promise<T>

const JOINED_TRANSACTION_SAVEPOINT = 'sirannon_joined'

async function unwindSavepoint(conn: SQLiteConnection): Promise<void> {
  try {
    await conn.exec(`ROLLBACK TO ${JOINED_TRANSACTION_SAVEPOINT}`)
    await conn.exec(`RELEASE ${JOINED_TRANSACTION_SAVEPOINT}`)
  } catch {}
}

/**
 * Executes `body` inside a savepoint on `conn` and returns its result, so that
 * its writes join the transaction that the caller already holds on the writer
 * and roll back on their own when `body` throws.
 *
 * Where no transaction is open, the savepoint opens one and its release
 * commits it, and a release that SQLite refuses raises `TRANSACTION_ERROR`.
 *
 * @param conn - The writer connection that the caller holds.
 * @param body - The work to execute inside the savepoint.
 * @returns What `body` returns.
 * @internal
 */
export async function runInSavepoint<T>(
  conn: SQLiteConnection,
  body: (txConn: SQLiteConnection) => Promise<T>,
): Promise<T> {
  await conn.exec(`SAVEPOINT ${JOINED_TRANSACTION_SAVEPOINT}`)
  let value: T
  try {
    value = await body(conn)
  } catch (err: unknown) {
    await unwindSavepoint(conn)
    throw err
  }
  try {
    await conn.exec(`RELEASE ${JOINED_TRANSACTION_SAVEPOINT}`)
  } catch (err: unknown) {
    await unwindSavepoint(conn)
    throw commitFailure(err)
  }
  return value
}
