import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'

/**
 * Returns how long, in milliseconds, a connection waits on a lock that another connection holds before SQLite reports
 * the database as busy.
 *
 * @param conn - The connection to read.
 * @returns The wait in milliseconds, or zero when the connection reports busy at once.
 * @internal
 */
export async function readBusyTimeout(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA busy_timeout')
  const row = await stmt.get<{ timeout: number | bigint }>()
  return row ? Number(row.timeout) : 0
}

/**
 * Sets how long, in milliseconds, a connection waits on a lock that another connection holds before SQLite reports
 * the database as busy.
 *
 * @param conn - The connection to change.
 * @param milliseconds - The wait, as a non-negative safe integer, where zero reports busy at once.
 * @returns A promise that settles once SQLite has applied the setting.
 * @internal
 */
export async function setBusyTimeout(conn: SQLiteConnection, milliseconds: number): Promise<void> {
  if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
    throw new SirannonError(
      `A busy timeout must be a non-negative integer, and ${milliseconds} is not`,
      'INTERNAL_ERROR',
    )
  }
  await conn.exec(`PRAGMA busy_timeout = ${milliseconds}`)
}
