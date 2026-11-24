/**
 * Starts watching one database file for writes, and returns the function that stops the watch, or `null` when the
 * runtime cannot watch that file.
 *
 * @internal
 */
export type DatabaseFileWatchStarter = (onChange: () => void, onLost: () => void) => (() => void) | null

/**
 * Tells every change poller of one database that the database may hold changes that the poller has not read yet.
 *
 * The signal fires when a writer-lock hold that may have committed a write ends, and when the driver reports a write
 * to the database file, which includes a write from another process. It watches the file only while a poller
 * listens. A poller reads the change log only after the signal fires, so a database that nobody writes to costs no
 * reads, and it falls back to reading on a fixed interval while
 * {@link DatabaseChangeSignal.detectsEveryWrite} is `false`.
 *
 * @internal
 */
export class DatabaseChangeSignal {
  private readonly listeners = new Set<() => void>()
  private stopWatching: (() => void) | null = null
  private watchLost = false

  /**
   * @param startWatch - Starts the file watch, or `null` when the driver cannot watch files.
   * @param fileSharedWithOtherConnections - `false` for a database that no other process or connection can open,
   * such as an in-memory database, whose every write arrives through the writer lock.
   */
  constructor(
    private readonly startWatch: DatabaseFileWatchStarter | null,
    private readonly fileSharedWithOtherConnections: boolean,
  ) {}

  /**
   * Returns `true` while the signal fires for every committed write, which holds when nothing outside this process
   * can write to the database or while the file watch runs.
   */
  get detectsEveryWrite(): boolean {
    return !this.fileSharedWithOtherConnections || this.stopWatching !== null
  }

  /**
   * Registers a function that the signal calls when the database may have changed, and starts the file watch for the
   * first one.
   *
   * @param listener - The function to call, which must return at once and do its work later.
   * @returns A function that removes the registration and stops the file watch after the last one.
   */
  listen(listener: () => void): () => void {
    this.listeners.add(listener)
    if (this.listeners.size === 1) this.watchFile()
    return () => {
      if (!this.listeners.delete(listener) || this.listeners.size > 0) return
      this.unwatchFile()
    }
  }

  /** Calls every listener, which a writer-lock release and the file watch do. */
  readonly notify = (): void => {
    for (const listener of this.listeners) {
      try {
        listener()
      } catch {}
    }
  }

  private watchFile(): void {
    if (!this.fileSharedWithOtherConnections || this.startWatch === null || this.stopWatching !== null) return
    this.watchLost = false
    let stop: (() => void) | null = null
    try {
      stop = this.startWatch(this.notify, this.onWatchLost)
    } catch {
      stop = null
    }
    if (this.watchLost) {
      stop?.()
      return
    }
    this.stopWatching = stop
  }

  private unwatchFile(): void {
    const stop = this.stopWatching
    this.stopWatching = null
    stop?.()
  }

  private readonly onWatchLost = (): void => {
    this.watchLost = true
    this.unwatchFile()
    this.notify()
  }
}
