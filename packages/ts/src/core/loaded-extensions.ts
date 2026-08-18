import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { ExtensionError } from './errors.js'
import { loadExtension } from './extension-loader.js'

function forwardingConnection(connection: SQLiteConnection, onClose: () => void): SQLiteConnection {
  const { loadExtension, runBatch, runBatchSummary, runGroup } = connection
  const forwarding: SQLiteConnection = {
    exec: sql => connection.exec(sql),
    prepare: sql => connection.prepare(sql),
    transaction: fn => connection.transaction(fn),
    close: async () => {
      onClose()
      await connection.close()
    },
  }
  if (loadExtension) forwarding.loadExtension = path => loadExtension.call(connection, path)
  if (runBatch) forwarding.runBatch = (sql, paramsBatch) => runBatch.call(connection, sql, paramsBatch)
  if (runBatchSummary) {
    forwarding.runBatchSummary = (sql, paramsBatch) => runBatchSummary.call(connection, sql, paramsBatch)
  }
  if (runGroup) forwarding.runGroup = units => runGroup.call(connection, units)
  return forwarding
}

/**
 * Holds the resolved path of every compiled extension a database has loaded,
 * along with every connection this database opened beyond its pool, so each of
 * them can call the extension's functions. SQLite scopes a loaded extension to
 * the connection that loaded it, and a database opens a fresh connection for
 * each consistent snapshot read and one more for its live queries.
 *
 * Loading and opening run one at a time, so a connection opened while a load is
 * in flight either loads that extension on the way in or is one of the
 * connections the load reaches.
 *
 * @internal
 */
export class LoadedExtensions {
  private readonly resolvedPaths: string[] = []
  private readonly openedConnections = new Set<SQLiteConnection>()
  private queue: Promise<unknown> = Promise.resolve()

  constructor(private readonly driver: SQLiteDriver) {}

  private runInTurn<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.queue.then(operation, operation)
    this.queue = result.catch(() => undefined)
    return result
  }

  private async loadRecorded(connection: SQLiteConnection): Promise<void> {
    for (const resolvedPath of this.resolvedPaths) {
      if (!connection.loadExtension) {
        throw new ExtensionError(resolvedPath, 'The current driver opened a connection with no extension loading call')
      }
      await connection.loadExtension(resolvedPath)
    }
  }

  /** Loads an extension onto the pool's connections and every connection opened beyond it, then records it. */
  load(poolConnections: readonly SQLiteConnection[], extensionPath: string): Promise<void> {
    return this.runInTurn(async () => {
      const resolved = await loadExtension(this.driver, [...poolConnections, ...this.openedConnections], extensionPath)
      if (!this.resolvedPaths.includes(resolved)) this.resolvedPaths.push(resolved)
    })
  }

  /**
   * Opens a connection, loads every recorded extension onto it, and returns a
   * connection that forwards to it. Closing the returned connection stops the
   * tracking and closes the one underneath.
   *
   * @param openConnection - Opens the connection this database needs.
   * @returns The connection to read and write through.
   */
  open(openConnection: () => Promise<SQLiteConnection>): Promise<SQLiteConnection> {
    return this.runInTurn(async () => {
      const connection = await openConnection()
      try {
        await this.loadRecorded(connection)
      } catch (err) {
        await connection.close().catch(() => undefined)
        throw err
      }

      const tracked = forwardingConnection(connection, () => {
        this.openedConnections.delete(tracked)
      })
      this.openedConnections.add(tracked)
      return tracked
    })
  }
}
