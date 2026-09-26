import type { Database } from './database.js'

/** The outcome of taking one database offline.
 * @internal
 */
export type OfflineOutcome<T> =
  | {
      /** True when the action returned. */
      ok: true
      /** The value that the action returned. */
      value: T
      /** The error that the reopen threw, when it threw; the action's result is still valid. */
      reopenFailure?: unknown
    }
  | {
      /** False when the action threw. */
      ok: false
      /** The error that the action threw. */
      failure: unknown
      /** The error that the reopen threw, when it threw. */
      reopenFailure?: unknown
    }

/** One database that Sirannon takes offline while it replaces the database file.
 * @internal
 */
export interface DatabaseOffline<T> {
  /** The open database to close. */
  database: Database
  /** The file that the database was opened from, which the action works on. */
  path: string
  /** The function that Sirannon calls once the database is closed and no connection is open on its file. */
  action: (path: string) => Promise<T>
  /** The function that opens the database again under the same identifier, with its earlier settings. */
  reopen: () => Promise<unknown>
}

/**
 * Closes one database, calls an action on its file, and then opens that
 * database again.
 *
 * A restore rebuilds a database at its current path, and no connection may be
 * open on that file while Sirannon replaces its bytes, so this function closes
 * the database first. Sirannon reopens the database whether the action
 * succeeded or failed, so that after a failed action the caller still has a
 * database to query, along with the error that the action threw.
 *
 * When the close itself fails, this function throws that error and opens
 * nothing, because the old connections may still be open on the file, and a
 * second runtime over the same file would put two writers on one database. The
 * registry then has no database open under the identifier.
 *
 * The `ok` field is true only for an action that returned, because an action
 * that returns `undefined`, `null`, `0`, or `false` has still succeeded.
 *
 * The outcome includes both the action's result and any reopen failure, because
 * a restore that replaced the data and then failed to reopen the database has
 * done both, and a caller that saw only the reopen failure would believe that
 * its data was untouched.
 *
 * @param offline - The database, its path, the action, and the function that reopens the database.
 * @returns Whether the action returned, the value that it returned or the error that it threw, and any error that the reopen threw.
 *
 * @internal
 */
export async function takeDatabaseOffline<T>(offline: DatabaseOffline<T>): Promise<OfflineOutcome<T>> {
  await offline.database.close()

  let outcome: OfflineOutcome<T>
  try {
    outcome = { ok: true, value: await offline.action(offline.path) }
  } catch (err) {
    outcome = { ok: false, failure: err }
  }

  try {
    await offline.reopen()
  } catch (err) {
    outcome.reopenFailure = err
  }

  return outcome
}
