import type { ChangeRetentionOptions } from './cdc/device-retention.js'
import { closeDatabaseRuntime, type DatabaseRuntime } from './database-create.js'
import { ReadOnlyError, SirannonError } from './errors.js'
import type { DatabaseOptions } from './types.js'

/**
 * Gives every open database its identity, its file, the checks that each
 * method makes before it does any work, and the methods that close the
 * database.
 *
 * Open a database through {@link Sirannon.open}, which returns a
 * {@link Database} that extends this class.
 *
 * @public
 */
export class DatabaseLifecycle {
  /** The identifier that this database was opened under. */
  readonly id: string
  /** The file path of the SQLite database. */
  readonly path: string
  /** Whether this database rejects writes. */
  readonly readOnly: boolean

  /**
   * The retention settings that this database was opened with, which form an empty object when the caller set none.
   *
   * @internal
   */
  readonly changeRetention: ChangeRetentionOptions

  /**
   * The pool, locks, and controllers that this database sends its work through.
   *
   * @internal
   */
  protected readonly runtime: DatabaseRuntime
  /**
   * The functions that Sirannon calls while this database closes.
   *
   * @internal
   */
  protected readonly closeListeners: (() => void | Promise<void>)[] = []
  private closing = false

  protected constructor(id: string, path: string, runtime: DatabaseRuntime, options?: DatabaseOptions) {
    this.id = id
    this.path = path
    this.runtime = runtime
    this.readOnly = options?.readOnly ?? false
    this.changeRetention = {
      ...(options?.cdcRetention === undefined ? {} : { cdcRetention: options.cdcRetention }),
      ...(options?.deviceCursorRetention === undefined ? {} : { deviceCursorRetention: options.deviceCursorRetention }),
      ...(options?.maxChangesHeldForDevice === undefined
        ? {}
        : { maxChangesHeldForDevice: options.maxChangesHeldForDevice }),
    }
  }

  /**
   * Registers a function that Sirannon calls while this database closes.
   *
   * @param fn - The function to call during {@link DatabaseLifecycle.close}.
   *
   * @internal
   */
  addCloseListener(fn: () => void | Promise<void>): void {
    this.ensureNotClosed()
    this.closeListeners.push(fn)
  }

  /**
   * Closes every connection that this database opened, and stops delivering changes to its subscriptions.
   */
  async close(): Promise<void> {
    if (this.closing) return
    this.closing = true
    await closeDatabaseRuntime(this.runtime, this.closeListeners)
  }

  /**
   * True once {@link DatabaseLifecycle.close} has started on this database.
   */
  get closed(): boolean {
    return this.closing
  }

  /**
   * The number of read connections in the pool.
   */
  get readerCount(): number {
    return this.runtime.pool.readerCount
  }

  /**
   * Throws when this database is closed, is loading a snapshot, or rejects writes.
   *
   * @internal
   */
  protected ensureWritable(): void {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
  }

  /**
   * Throws when this database is closed or is loading a snapshot.
   *
   * @internal
   */
  protected ensureOpen(): void {
    this.ensureNotClosed()
    if (this.runtime.sync.snapshotLoadBlocked) {
      throw new SirannonError(
        `Database '${this.id}' is replacing its data from a sync snapshot; retry once the snapshot load completes`,
        'SNAPSHOT_IN_PROGRESS',
      )
    }
  }

  /**
   * Throws when this database is closed.
   *
   * @internal
   */
  protected ensureNotClosed(): void {
    if (this.closing) {
      throw new SirannonError(`Database '${this.id}' is closed`, 'DATABASE_CLOSED')
    }
  }
}
