import type { Database } from './database.js'

/** What one database taken out of service produced.
 * @internal
 */
export type OfflineOutcome<T> =
  | {
      /** True where the action returned. */
      ok: true
      /** What the action produced. */
      value: T
      /** What the reopen threw, where it threw. The action's own result still stands. */
      reopenFailure?: unknown
    }
  | {
      /** False where the action threw. */
      ok: false
      /** What the action threw. */
      failure: unknown
      /** What the reopen threw, where it threw. */
      reopenFailure?: unknown
    }

/** One database taken out of service while Sirannon replaces its file.
 * @internal
 */
export interface DatabaseOffline<T> {
  /** The open database to close. */
  database: Database
  /** File it was opened from, which the action works against. */
  path: string
  /** Called with the database closed and no connection open on its file. */
  action: (path: string) => Promise<T>
  /** Opens it again under the same identifier, with the settings it had before. */
  reopen: () => Promise<unknown>
}

/**
 * Closes one database, calls an action against the file behind it, and then
 * opens that database again.
 *
 * A restore rebuilds a database at the path it already occupies, and no
 * connection may be open on that file while Sirannon replaces its bytes, which
 * is why the close comes first. The action then has the path to itself. The
 * database opens again whether that action succeeded or failed, so an action
 * that failed still leaves the caller with a database it can query, and the
 * caller receives the error the action threw.
 *
 * A close that fails is a different case. The old connections may still be open
 * on the file, and a second runtime over the same file would put two writers on
 * one database, so that failure passes straight to the caller and the registry
 * has nothing open under the identifier.
 *
 * The `ok` field separates an action that returned from one that threw, since
 * an action producing `undefined`, `null`, `0`, or `false` has still succeeded.
 *
 * The action's result and the reopen's failure are both reported. A restore
 * that replaced the data and then failed to open the database again has done
 * both of those things, and a caller shown only the second would believe its
 * data untouched.
 *
 * @param offline - The database, its path, the action, and how to open it again.
 * @returns Whether the action returned, what it produced or threw, and what the reopen threw.
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
