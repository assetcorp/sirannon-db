import { closeDatabaseRuntime, type DatabaseRuntime } from './database-create.js'
import { ReadOnlyError, SirannonError } from './errors.js'
import type { DatabaseOptions } from './types.js'

/**
 * What every open database carries whatever statements run on it: which file
 * it is, the runtime behind it, the guards each member runs before it touches
 * that runtime, and the members that close it.
 *
 * Open a database through {@link Sirannon.open}, which hands back a
 * {@link Database} built on this.
 *
 * @public
 */
export class DatabaseLifecycle {
  /** Identifier this database was opened under. */
  readonly id: string
  /** File path of the SQLite database. */
  readonly path: string
  /** Whether this database refuses writes. */
  readonly readOnly: boolean

  /**
   * Pool, locks, and controllers this database runs its work through.
   *
   * @internal
   */
  protected readonly runtime: DatabaseRuntime
  /**
   * Functions that run while this database closes.
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
  }

  /**
   * Registers a function that runs while this database closes.
   *
   * @param fn - Called during {@link DatabaseLifecycle.close}.
   *
   * @internal
   */
  addCloseListener(fn: () => void | Promise<void>): void {
    this.ensureNotClosed()
    this.closeListeners.push(fn)
  }

  /**
   * Closes every connection this database holds and ends its subscriptions.
   */
  async close(): Promise<void> {
    if (this.closing) return
    this.closing = true
    await closeDatabaseRuntime(this.runtime, this.closeListeners)
  }

  /**
   * Whether this database has been closed.
   */
  get closed(): boolean {
    return this.closing
  }

  /**
   * Number of read connections the pool holds.
   */
  get readerCount(): number {
    return this.runtime.pool.readerCount
  }

  /**
   * Refuses the call when this database is closed, is loading a snapshot, or refuses writes.
   *
   * @internal
   */
  protected ensureWritable(): void {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
  }

  /**
   * Refuses the call when this database is closed or is loading a snapshot.
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
   * Refuses the call when this database is closed.
   *
   * @internal
   */
  protected ensureNotClosed(): void {
    if (this.closing) {
      throw new SirannonError(`Database '${this.id}' is closed`, 'DATABASE_CLOSED')
    }
  }
}
