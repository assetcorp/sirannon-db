import type { SQLiteConnection, SQLiteDriver, SynchronousLevel } from './driver/types.js'
import { EncryptionError } from './encryption/errors.js'
import type { EncryptionKeyring, KeyedOpenOptions } from './encryption/keyring.js'
import { ConnectionPoolError, SirannonError } from './errors.js'
import type { WorkerHostOptions } from './worker/host.js'

/**
 * Settings that {@link ConnectionPool.create} uses to open the writer and reader connections.
 *
 * @internal
 */
export interface ConnectionPoolOptions {
  driver: SQLiteDriver
  path: string
  readOnly?: boolean
  readPoolSize?: number
  walMode?: boolean
  synchronous?: SynchronousLevel
  walAutoCheckpoint?: number
  useWriterWorker?: boolean
  workerHostOptions?: WorkerHostOptions
  keyring?: EncryptionKeyring
}

interface PoolConnections {
  writer: SQLiteConnection | null
  readers: SQLiteConnection[]
}

async function closeAllSilently(connections: (SQLiteConnection | null)[]): Promise<void> {
  for (const conn of connections) {
    if (!conn) continue
    try {
      await conn.close()
    } catch {}
  }
}

function writerHostOptions(options: ConnectionPoolOptions): WorkerHostOptions | undefined {
  const keyring = options.keyring
  if (!keyring?.encrypted) return options.workerHostOptions
  return { ...options.workerHostOptions, encryptionKey: () => keyring.freshKeyBytes() }
}

async function openWriter(options: ConnectionPoolOptions, keyed: KeyedOpenOptions): Promise<SQLiteConnection> {
  const { driver, path, walMode = true, synchronous } = options
  const writerOptions = {
    walMode,
    synchronous,
    ...(options.walAutoCheckpoint === undefined ? {} : { walAutoCheckpoint: options.walAutoCheckpoint }),
  }
  if (!options.useWriterWorker) return driver.open(path, { ...writerOptions, ...keyed })
  if (!driver.startWriterHost) {
    throw new SirannonError(
      'writerWorker is enabled but the driver does not carry a worker entry; use a driver that supports it or disable writerWorker',
      'WRITER_WORKER_UNSUPPORTED',
    )
  }
  return driver.startWriterHost(path, writerOptions, writerHostOptions(options))
}

async function openConnections(options: ConnectionPoolOptions): Promise<PoolConnections> {
  const { driver, path, readOnly = false, readPoolSize = 4 } = options
  const open = async (keyed: KeyedOpenOptions): Promise<PoolConnections> => {
    let writer: SQLiteConnection | null = null
    const readers: SQLiteConnection[] = []
    try {
      if (!readOnly) writer = await openWriter(options, keyed)
      const poolSize = driver.capabilities.multipleConnections ? Math.max(readPoolSize, 1) : 0
      for (let i = 0; i < poolSize; i++) {
        readers.push(await driver.open(path, { readonly: true, walMode: false, ...keyed }))
      }
    } catch (err) {
      await closeAllSilently([...readers, writer])
      throw err
    }
    return { writer, readers }
  }
  return options.keyring ? options.keyring.openWith(open) : open({})
}

/**
 * Opens one writer connection and a set of reader connections for a single database file, and returns the readers in rotation.
 *
 * @internal
 */
export class ConnectionPool {
  private writer: SQLiteConnection | null
  private readers: SQLiteConnection[]
  private readerIndex = 0
  private closed = false
  private isSuspended = false

  private constructor(
    private readonly options: ConnectionPoolOptions,
    opened: PoolConnections,
  ) {
    this.writer = opened.writer
    this.readers = opened.readers
  }

  static async create(options: ConnectionPoolOptions): Promise<ConnectionPool> {
    return new ConnectionPool(options, await openConnections(options))
  }

  private ensureAvailable(): void {
    if (this.closed) {
      throw new ConnectionPoolError('Connection pool is closed')
    }
    if (this.isSuspended) {
      throw new EncryptionError(
        'The database is replacing its file to finish a key change, so its connections reopen once the replacement ends; retry the call',
        'ENCRYPTION_CHANGE_IN_PROGRESS',
      )
    }
  }

  acquireReader(): SQLiteConnection {
    this.ensureAvailable()
    if (this.readers.length === 0) {
      if (!this.writer) {
        throw new ConnectionPoolError('No connections available')
      }
      return this.writer
    }
    const reader = this.readers[this.readerIndex % this.readers.length]
    this.readerIndex = (this.readerIndex + 1) % this.readers.length
    return reader
  }

  acquireWriter(): SQLiteConnection {
    this.ensureAvailable()
    if (!this.writer) {
      throw new ConnectionPoolError('No write connection available (database is read-only)')
    }
    return this.writer
  }

  /** Returns the writer and every reader, so that the caller can apply an operation to every connection in the pool. */
  connections(): readonly SQLiteConnection[] {
    this.ensureAvailable()
    return this.writer ? [this.writer, ...this.readers] : [...this.readers]
  }

  get readerCount(): number {
    return this.readers.length
  }

  get isReadOnly(): boolean {
    return this.options.readOnly === true
  }

  /**
   * Closes every reader and leaves the writer open, so that a checkpoint on the writer can reach the end of the log.
   * Every acquire fails with `ENCRYPTION_CHANGE_IN_PROGRESS` until {@link ConnectionPool.reopen} returns.
   */
  async closeReaders(): Promise<void> {
    this.isSuspended = true
    const readers = this.readers
    this.readers = []
    await closeAllSilently(readers)
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

  /** Opens the writer and the readers again, under the key that the pool's keyring holds now. */
  async reopen(): Promise<void> {
    if (this.closed || !this.isSuspended) return
    await closeAllSilently([...this.readers, this.writer])
    this.readers = []
    this.writer = null
    const opened = await openConnections(this.options)
    this.writer = opened.writer
    this.readers = opened.readers
    this.readerIndex = 0
    this.isSuspended = false
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true

    const errors: unknown[] = []

    for (const reader of this.readers) {
      try {
        await reader.close()
      } catch (err) {
        errors.push(err)
      }
    }

    if (this.writer) {
      try {
        await this.writer.close()
      } catch (err) {
        errors.push(err)
      }
    }

    if (errors.length > 0) {
      throw new ConnectionPoolError(`Failed to close ${errors.length} connection(s)`)
    }
  }
}
