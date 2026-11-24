import { ConnectionIdleClock } from './connection-pool-idle.js'
import {
  type ConnectionPoolOptions,
  closeAllSilently,
  connectionIdleTimeoutOf,
  openConnections,
  openIdleClosedWriter,
  openLateReader,
  type PoolConnections,
  permanentReaderCountOf,
  readerCapacityOf,
} from './connection-pool-open.js'
import { bothClosed, closeCountingFailures, poolUnavailableError, readsSettleWithin } from './connection-pool-state.js'
import type { SQLiteConnection } from './driver/types.js'
import { EncryptionError } from './encryption/errors.js'
import { ConnectionPoolError } from './errors.js'

/**
 * Opens one writer connection and a set of reader connections for a single database file, and returns the readers in rotation.
 *
 * A writable pool opens a reader when a read needs one and every open reader is busy. Once the pool goes the
 * connection idle timeout without a read, it closes its readers, and once it goes that long without a read or a
 * write, it can close its writer too, so that an idle database holds no connection. The writer lock opens the writer
 * again through {@link ConnectionPool.writerReady} before the next hold.
 *
 * @internal
 */
export class ConnectionPool {
  private writer: SQLiteConnection | null
  private readers: SQLiteConnection[]
  private readerIndex = 0
  private closed = false
  private isSuspended = false
  private readsInFlight = 0
  private readsSettled: (() => void) | null = null
  private reopenFailure: unknown = null
  private readerOpening: Promise<void> | null = null
  private generation = 0
  private readonly readerCapacity: number
  private readonly permanentReaders: number
  private readonly idleClock: ConnectionIdleClock
  private idleClosing: Promise<void> | null = null
  private writerClosedWhileIdle = false
  private writerOpening: Promise<void> | null = null

  private constructor(
    private readonly options: ConnectionPoolOptions,
    opened: PoolConnections,
  ) {
    this.writer = opened.writer
    this.readers = opened.readers
    this.readerCapacity = readerCapacityOf(options)
    this.permanentReaders = permanentReaderCountOf(options)
    this.idleClock = new ConnectionIdleClock(connectionIdleTimeoutOf(options), this.closeIdleConnections)
    this.idleClock.used()
  }

  static async create(options: ConnectionPoolOptions): Promise<ConnectionPool> {
    return new ConnectionPool(options, await openConnections(options))
  }

  private ensureAvailable(): void {
    const unavailable = poolUnavailableError(this.closed, this.isSuspended, this.reopenFailure)
    if (unavailable !== null) throw unavailable
  }

  private rejectionWhenUnavailable(): Promise<never> | null {
    const unavailable = poolUnavailableError(this.closed, this.isSuspended, this.reopenFailure)
    return unavailable === null ? null : Promise.reject(unavailable)
  }

  /**
   * Returns the next open reader in rotation, or the writer for a driver that opens one connection per file.
   *
   * @throws A `ConnectionPoolError` when the pool can hold readers and has none open, because a read has to wait for
   * {@link ConnectionPool.readerReady} first.
   */
  acquireReader(): SQLiteConnection {
    this.ensureAvailable()
    if (this.readers.length === 0) {
      if (this.readerCapacity > 0) {
        throw new ConnectionPoolError('No reader connection is open yet; wait for readerReady() before a read')
      }
      if (!this.writer) throw new ConnectionPoolError('No connections available')
      return this.writer
    }
    const reader = this.readers[this.readerIndex % this.readers.length]
    this.readerIndex = (this.readerIndex + 1) % this.readers.length
    return reader
  }

  /**
   * Returns `null` when the pool holds an open reader, and otherwise a promise that settles once it has opened one.
   * The pool opens no reader until a read needs one, so a database that nobody reads holds only its writer.
   *
   * @returns `null`, or the promise that settles once a reader is open.
   */
  readerReady(): Promise<void> | null {
    if (this.readers.length > 0 || this.readerCapacity === 0) return null
    const unavailable = this.rejectionWhenUnavailable()
    if (unavailable !== null) return unavailable
    this.readerOpening ??= this.addReader().finally(() => {
      this.readerOpening = null
    })
    return this.readerOpening
  }

  private async addReader(): Promise<void> {
    const generation = this.generation
    const reader = await openLateReader(this.options)
    let added = false
    const add = (): void => {
      if (this.closed || this.isSuspended || generation !== this.generation) return
      if (this.readers.length >= this.readerCapacity) return
      this.readers.push(reader)
      added = true
    }
    try {
      if (this.options.admitReader) await this.options.admitReader(reader, add)
      else add()
    } finally {
      if (!added) await closeAllSilently([reader])
    }
    if (!added && this.readers.length === 0) {
      this.ensureAvailable()
      throw new ConnectionPoolError('The pool closed the reader that it opened before a read could use it')
    }
  }

  private growReaders(): void {
    if (this.readerOpening !== null || this.readers.length >= this.readerCapacity) return
    this.readerOpening = this.addReader()
      .catch(() => undefined)
      .finally(() => {
        this.readerOpening = null
      })
  }

  /**
   * Returns the writer, which a caller takes only while it holds the writer lock.
   *
   * @throws A `ConnectionPoolError` on a read-only database, and when the pool closed the writer while idle and no
   * writer-lock hold has opened it again through {@link ConnectionPool.writerReady}.
   */
  acquireWriter(): SQLiteConnection {
    this.ensureAvailable()
    if (!this.writer) {
      if (this.writerClosedWhileIdle) {
        throw new ConnectionPoolError(
          'The database closed its idle writer, and the writer opens again only at the start of a writer-lock hold',
        )
      }
      throw new ConnectionPoolError('No write connection available (database is read-only)')
    }
    this.idleClock.used()
    return this.writer
  }

  /**
   * Returns `null` while the writer is open, and otherwise opens it again, under the key that the keyring holds now
   * and with the database's extensions, and returns the promise that settles once it is open. Every caller that asks
   * while the open runs receives the same promise, so the pool opens the writer once.
   *
   * @returns `null`, or the promise that settles once the writer is open.
   */
  writerReady(): Promise<void> | null {
    if (!this.writerClosedWhileIdle) return null
    const unavailable = this.rejectionWhenUnavailable()
    if (unavailable !== null) return unavailable
    this.writerOpening ??= this.openWriterAgain().finally(() => {
      this.writerOpening = null
    })
    return this.writerOpening
  }

  private async openWriterAgain(): Promise<void> {
    const generation = this.generation
    const writer = await openIdleClosedWriter(this.options)
    const assertCurrent = () => {
      if (this.closed || generation !== this.generation) throw new ConnectionPoolError('Connection pool is closed')
    }
    try {
      assertCurrent()
      await this.options.prepareReopened?.([writer])
      assertCurrent()
    } catch (err) {
      await closeAllSilently([writer])
      throw err
    }
    this.writer = writer
    this.writerClosedWhileIdle = false
    this.idleClock.used()
  }

  /**
   * Returns the next reader connection and keeps it open until the caller passes the lease back through
   * {@link ConnectionPool.releaseReader}, so that {@link ConnectionPool.closeReaders} closes no reader under a read.
   *
   * @returns The reader connection, which the caller releases exactly once when its read settles.
   */
  leaseReader(): SQLiteConnection {
    const reader = this.acquireReader()
    if (this.readsInFlight >= this.readers.length) this.growReaders()
    this.readsInFlight++
    return reader
  }

  /** Ends a lease that {@link ConnectionPool.leaseReader} returned. */
  releaseReader(): void {
    this.readsInFlight--
    if (this.readsInFlight > 0) return
    this.readsSettled?.()
    this.idleClock.used()
  }

  private readonly closeIdleConnections = (): void => {
    if (this.closed || this.isSuspended || this.readsInFlight > 0) return
    if (this.readerOpening !== null || this.writerOpening !== null || this.idleClosing !== null) {
      this.idleClock.checkAgainLater()
      return
    }
    this.retireIdleReaders()
    const closeExclusively = this.options.closeIdleWriterExclusively
    if (closeExclusively === undefined || this.writer === null) return
    if (this.options.keepsWriterOpen?.() === true) {
      this.idleClock.checkAgainLater()
      return
    }
    closeExclusively(this.closeIdleWriter)
  }

  private retireIdleReaders(): void {
    const retired = this.readers.splice(this.permanentReaders)
    this.readerIndex = 0
    if (retired.length === 0) return
    this.trackIdleClose(closeAllSilently(retired))
  }

  private readonly closeIdleWriter = async (): Promise<void> => {
    const writer = this.writer
    if (this.closed || this.isSuspended || writer === null || this.readsInFlight > 0) return
    if (this.options.keepsWriterOpen?.() === true || !this.idleClock.expired) {
      this.idleClock.checkAgainLater()
      return
    }
    this.retireIdleReaders()
    this.writer = null
    this.writerClosedWhileIdle = true
    const closing = closeAllSilently([writer])
    this.trackIdleClose(closing)
    await closing
  }

  private trackIdleClose(closing: Promise<void>): void {
    const tracked = bothClosed(this.idleClosing, closing)
    this.idleClosing = tracked
    void tracked.finally(() => {
      if (this.idleClosing === tracked) this.idleClosing = null
    })
  }

  /**
   * Returns what a read on the next reader connection returns, and keeps that connection open until the read settles,
   * so that {@link ConnectionPool.closeReaders} closes no reader under a read.
   *
   * @param read - The read to run on the reader.
   * @returns The value of the read.
   */
  async withReader<T>(read: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    const opening = this.readerReady()
    if (opening !== null) await opening
    const reader = this.leaseReader()
    try {
      return await read(reader)
    } finally {
      this.releaseReader()
    }
  }

  /** Returns the writer and every reader, so that the caller can apply an operation to every connection in the pool. */
  connections(): readonly SQLiteConnection[] {
    this.ensureAvailable()
    return this.writer ? [this.writer, ...this.readers] : [...this.readers]
  }

  /** Returns what `operation` returns, and keeps every connection open until it settles, as a read keeps its reader. */
  async whileConnectionsOpen<T>(operation: () => Promise<T>): Promise<T> {
    this.readsInFlight++
    try {
      return await operation()
    } finally {
      this.releaseReader()
    }
  }

  /**
   * The most readers that the pool opens, which it opens as reads need them, or zero while a key change has closed
   * them and for a driver that opens one connection per file.
   */
  get readerCount(): number {
    return this.isSuspended ? 0 : this.readerCapacity
  }

  get isReadOnly(): boolean {
    return this.options.readOnly === true
  }

  /**
   * Waits for every read on a reader to settle, then closes every reader and leaves the writer open, so that a
   * checkpoint on the writer can reach the end of the log. Every acquire fails with `ENCRYPTION_CHANGE_IN_PROGRESS`
   * until {@link ConnectionPool.reopen} returns.
   *
   * @param drainTimeoutMs - How long to wait for the reads to settle.
   * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when a read runs past the timeout, which leaves the
   * readers open and the pool available.
   */
  async closeReaders(drainTimeoutMs: number): Promise<void> {
    this.isSuspended = true
    this.generation++
    const readers = this.readers
    this.readers = []
    await this.readerOpening?.catch(() => undefined)
    await this.idleClosing
    if (!(await this.readsDrained(drainTimeoutMs))) {
      this.readers = readers
      this.isSuspended = false
      throw new EncryptionError(
        `A read held a reader connection for longer than ${drainTimeoutMs} ms, so the key change stopped before it replaced the file`,
        'ENCRYPTION_CHANGE_ABORTED',
      )
    }
    await closeAllSilently(readers)
  }

  private readsDrained(timeoutMs: number): Promise<boolean> {
    if (this.readsInFlight === 0) return Promise.resolve(true)
    return readsSettleWithin(settled => {
      this.readsSettled = settled
    }, timeoutMs)
  }

  /** Closes the writer after {@link ConnectionPool.closeReaders}. */
  async closeWriter(): Promise<void> {
    const writer = this.writer
    this.writer = null
    if (writer) await writer.close()
  }

  /** `true` from {@link ConnectionPool.closeReaders} until {@link ConnectionPool.reopen} returns. */
  get suspended(): boolean {
    return this.isSuspended
  }

  /**
   * Opens the writer and the readers again, under the key that the pool's keyring holds now, and loads into them
   * every extension that the database loaded into the connections they replace. When the connections fail to open,
   * every acquire fails with that error until a later reopen succeeds.
   */
  async reopen(): Promise<void> {
    if (this.closed || !this.isSuspended) return
    this.generation++
    await this.writerOpening?.catch(() => undefined)
    await closeAllSilently([...this.readers, this.writer])
    this.readers = []
    this.writer = null
    let opened: PoolConnections
    try {
      opened = await openConnections(this.options)
    } catch (err) {
      this.reopenFailure = err
      throw err
    }
    this.reopenFailure = null
    this.writer = opened.writer
    this.writerClosedWhileIdle = false
    this.readers = opened.readers
    this.readerIndex = 0
    this.idleClock.used()
    try {
      await this.options.prepareReopened?.(opened.writer ? [opened.writer, ...opened.readers] : opened.readers)
    } finally {
      this.isSuspended = false
    }
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.generation++
    this.idleClock.stop()
    await this.readerOpening?.catch(() => undefined)
    await this.writerOpening?.catch(() => undefined)
    await this.idleClosing

    const failures = await closeCountingFailures(this.writer ? [...this.readers, this.writer] : this.readers)
    if (failures > 0) throw new ConnectionPoolError(`Failed to close ${failures} connection(s)`)
  }
}
