import type { DatabaseChangeSignal } from './cdc/change-signal.js'
import type { PruneBoundarySource } from './cdc/prune-boundaries.js'
import { DatabaseBackups } from './database-backups.js'
import type { SQLiteConnection } from './driver/types.js'

/**
 * Gives an open database the calls through which the server's change feeds read its change log, maintain its change
 * log, and learn when it changes.
 *
 * Open a database through {@link Sirannon.open}, which returns a {@link Database} that extends this class.
 *
 * @public
 */
export class DatabaseChangeFeeds extends DatabaseBackups {
  /**
   * Executes a change-log maintenance write, such as pruning, on the shared
   * writer connection under the writer lock, so that it queues behind
   * application writes and never contends with them for SQLite's write lock,
   * which would block the event loop for the `busy_timeout`. The write must add
   * nothing to the change log, because it does not fire the change signal.
   *
   * @internal
   */
  async runCdcMaintenance(op: (writer: SQLiteConnection) => Promise<unknown>): Promise<void> {
    if (this.closed) return
    await this.runtime.writerLock.runWithoutChangeSignal(() => op(this.runtime.pool.acquireWriter()))
  }

  /**
   * Runs a read of the change log on a reader connection, or on the writer under the writer lock while the database
   * has no reader or a key change has closed its readers, so that a change feed never makes a write wait.
   *
   * @param read - The read, which must not write.
   * @returns What `read` returns.
   * @internal
   */
  readChangeLog<T>(read: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    try {
      this.ensureNotClosed()
    } catch (err) {
      return Promise.reject(err)
    }
    return this.runtime.cdc.readChangeLog(read)
  }

  /**
   * The signal that fires when this database may hold changes that a change feed has not read yet, after a write
   * through this database and after the driver reports a write to the file from any process.
   *
   * @internal
   */
  get changeSignal(): DatabaseChangeSignal {
    return this.runtime.cdc.changeSignal
  }

  /**
   * The shortest time between two reads of the change log, in milliseconds, which is the `cdcPollInterval` that this
   * database was opened with.
   *
   * @internal
   */
  get cdcPollInterval(): number {
    return this.runtime.cdc.pollInterval
  }

  /**
   * Keeps the change-log entries that a component outside this database still needs, such as the changes that the replication engine's peers have yet to acknowledge, out of reach of this database's pruning.
   *
   * @param source - The component that holds the entries back.
   * @param seq - The highest change-log sequence that pruning can remove for this component, or `null` to release the hold.
   * @internal
   */
  holdChangeLogPruning(source: PruneBoundarySource, seq: bigint | null): void {
    const tracker = this.runtime.cdc.changeTracker
    if (tracker === null) return
    if (seq === null) tracker.clearPruneBoundary(source)
    else tracker.setPruneBoundary(source, seq)
  }
}
