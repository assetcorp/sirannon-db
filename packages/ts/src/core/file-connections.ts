import type { OpenOptions, SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { EncryptionError } from './encryption/errors.js'
import type { EncryptionKeyring } from './encryption/keyring.js'
import type { LoadedExtensions } from './loaded-extensions.js'

/**
 * A part of the database that keeps a connection to the file open between calls, and closes and reopens it when a
 * key change replaces the file.
 *
 * @internal
 */
export interface FileConnectionHolder {
  /** Closes the held connection and stops the work that uses it. */
  release(): Promise<void>
  /** Opens a new connection to the file that the key change left in place, and resumes the work that uses it. */
  reopen(): Promise<void>
}

/**
 * Opens every connection to a database file beyond the pool, under the database's current key and with its loaded
 * extensions, and tracks each one, so that a key change can close all of them before it replaces the file and open
 * them again afterwards.
 *
 * @internal
 */
export class DatabaseFileConnections {
  private active = 0
  private readonly holders = new Set<FileConnectionHolder>()
  private replacement: { done: Promise<void>; finish: () => void } | null = null
  private drained: (() => void) | null = null

  constructor(
    private readonly driver: SQLiteDriver,
    private readonly path: string,
    private readonly keyring: EncryptionKeyring,
    private readonly extensions: LoadedExtensions,
  ) {}

  /**
   * Opens a connection to the database file, after any file replacement under way has ended.
   *
   * @param options - The open settings, without a key, which this method adds.
   * @returns The connection, which the caller closes.
   */
  async open(options: OpenOptions): Promise<SQLiteConnection> {
    while (this.replacement !== null) await this.replacement.done
    this.active++
    let connection: SQLiteConnection
    try {
      connection = await this.extensions.open(() =>
        this.keyring.openWith(keyed => this.driver.open(this.path, { ...options, ...keyed })),
      )
    } catch (err) {
      this.settle()
      throw err
    }
    const close = connection.close
    let settled = false
    connection.close = async () => {
      if (settled) return close.call(connection)
      settled = true
      try {
        await close.call(connection)
      } finally {
        this.settle()
      }
    }
    return connection
  }

  private settle(): void {
    this.active--
    if (this.active === 0) this.drained?.()
  }

  /**
   * Registers a part of the database that keeps a connection open between calls.
   *
   * @param holder - The release and reopen calls of that part.
   * @returns A function that removes the registration.
   */
  hold(holder: FileConnectionHolder): () => void {
    this.holders.add(holder)
    return () => {
      this.holders.delete(holder)
    }
  }

  /**
   * Makes later opens wait, has every holder close its connection, and waits until every other connection to the
   * file has closed.
   *
   * @param timeoutMs - How long to wait for the connections that a call holds for the duration of one read.
   * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when a connection stays open past the deadline.
   */
  async closeForReplacement(timeoutMs: number): Promise<void> {
    let finish = () => {}
    const done = new Promise<void>(resolve => {
      finish = resolve
    })
    this.replacement = { done, finish }
    for (const holder of [...this.holders]) {
      await holder.release().catch(() => undefined)
    }
    if (this.active === 0) return
    let timer: ReturnType<typeof setTimeout> | undefined
    const closed = await Promise.race([
      new Promise<boolean>(resolve => {
        this.drained = () => resolve(true)
      }),
      new Promise<boolean>(resolve => {
        timer = setTimeout(() => resolve(false), timeoutMs)
      }),
    ])
    clearTimeout(timer)
    this.drained = null
    if (!closed) {
      throw new EncryptionError(
        `A read held a connection to the database file for longer than ${timeoutMs} ms, so the key change stopped before it replaced the file`,
        'ENCRYPTION_CHANGE_ABORTED',
      )
    }
  }

  /** Lets opens proceed again, and has every holder open its connection to the file that is now in place. */
  async reopenAfterReplacement(): Promise<void> {
    const replacement = this.replacement
    this.replacement = null
    replacement?.finish()
    for (const holder of [...this.holders]) {
      await holder.reopen().catch(() => undefined)
    }
  }
}
