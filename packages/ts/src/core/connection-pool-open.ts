import type { SQLiteConnection, SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { EncryptionKeyring, KeyedOpenOptions } from './encryption/keyring.js'
import { SirannonError } from './errors.js'
import type { WorkerHostOptions } from './worker/host.js'

const DEFAULT_READ_POOL_SIZE = 4
const DEFAULT_CONNECTION_IDLE_TIMEOUT_MS = 30_000

/**
 * Settings that a connection pool uses to open the writer and reader connections.
 *
 * @internal
 */
export interface ConnectionPoolOptions {
  driver: SQLiteDriver
  path: string
  readOnly?: boolean
  readPoolSize?: number
  connectionIdleTimeout?: number
  /**
   * Runs the close of an idle writer while the caller holds the writer lock, so that the close waits for every write
   * under way and every later write waits for the close. A pool without it keeps its writer open.
   */
  closeIdleWriterExclusively?: (close: () => Promise<void>) => void
  /** Returns `true` while something needs the writer to stay open however long it sits idle. */
  keepsWriterOpen?: () => boolean
  walMode?: boolean
  synchronous?: SynchronousLevel
  walAutoCheckpoint?: number
  useWriterWorker?: boolean
  workerHostOptions?: WorkerHostOptions
  keyring?: EncryptionKeyring
  prepareReopened?: (connections: readonly SQLiteConnection[]) => Promise<void>
  /**
   * Prepares a reader that the pool opened after its other connections, such as by loading the database's
   * extensions into it, and then calls `add`, which puts the reader in the pool.
   */
  admitReader?: (reader: SQLiteConnection, add: () => void) => Promise<void>
}

/**
 * The connections that a pool opens when it opens the file.
 *
 * @internal
 */
export interface PoolConnections {
  writer: SQLiteConnection | null
  readers: SQLiteConnection[]
}

/**
 * Closes each connection and ignores every failure.
 *
 * @internal
 */
export async function closeAllSilently(connections: (SQLiteConnection | null)[]): Promise<void> {
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

/**
 * Returns the most readers that a pool holds, which is zero for a driver that opens one connection per file.
 *
 * @internal
 */
export function readerCapacityOf(options: ConnectionPoolOptions): number {
  return options.driver.capabilities.multipleConnections
    ? Math.max(options.readPoolSize ?? DEFAULT_READ_POOL_SIZE, 1)
    : 0
}

/**
 * Returns the readers that a pool keeps open however long they sit idle: one for a read-only pool, which has no
 * writer to read through, and none for a writable pool.
 *
 * @internal
 */
export function permanentReaderCountOf(options: ConnectionPoolOptions): number {
  return options.readOnly === true ? Math.min(1, readerCapacityOf(options)) : 0
}

/**
 * Returns how long, in milliseconds, a pool has to go without a read before it closes the readers above
 * {@link permanentReaderCountOf}, and without a read or a write before it closes its writer, or `0` when the pool
 * keeps its connections open.
 *
 * @internal
 */
export function connectionIdleTimeoutOf(options: ConnectionPoolOptions): number {
  const timeout = options.connectionIdleTimeout ?? DEFAULT_CONNECTION_IDLE_TIMEOUT_MS
  return Number.isFinite(timeout) && timeout > 0 ? timeout : 0
}

/**
 * Opens the writer again, under the key that the pool's keyring holds now, after the pool closed it while idle.
 *
 * @internal
 */
export async function openIdleClosedWriter(options: ConnectionPoolOptions): Promise<SQLiteConnection> {
  const open = (keyed: KeyedOpenOptions) => openWriter(options, keyed)
  return options.keyring ? options.keyring.openWith(open) : open({})
}

function openReader(options: ConnectionPoolOptions, keyed: KeyedOpenOptions): Promise<SQLiteConnection> {
  return options.driver.open(options.path, { readonly: true, walMode: false, ...keyed })
}

/**
 * Opens the writer, and one reader for a read-only pool, which has no writer to check the file with. A writable pool
 * opens its readers as reads need them.
 *
 * @internal
 */
export async function openConnections(options: ConnectionPoolOptions): Promise<PoolConnections> {
  const readOnly = options.readOnly ?? false
  const eagerReaders = permanentReaderCountOf(options)
  const open = async (keyed: KeyedOpenOptions): Promise<PoolConnections> => {
    let writer: SQLiteConnection | null = null
    const readers: SQLiteConnection[] = []
    try {
      if (!readOnly) writer = await openWriter(options, keyed)
      for (let i = 0; i < eagerReaders; i++) {
        readers.push(await openReader(options, keyed))
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
 * Opens one more reader under the key that the pool's keyring holds now.
 *
 * @internal
 */
export async function openLateReader(options: ConnectionPoolOptions): Promise<SQLiteConnection> {
  const open = (keyed: KeyedOpenOptions) => openReader(options, keyed)
  return options.keyring ? options.keyring.openWith(open) : open({})
}
