import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { ConnectionPoolError } from './errors.js'

export interface ConnectionPoolOptions {
  driver: SQLiteDriver
  path: string
  readOnly?: boolean
  readPoolSize?: number
  walMode?: boolean
}

async function closeAllSilently(connections: (SQLiteConnection | null)[]): Promise<void> {
  for (const conn of connections) {
    if (!conn) continue
    try {
      await conn.close()
    } catch {
      // Best-effort cleanup; the original error takes priority
    }
  }
}

export class ConnectionPool {
  private readonly writer: SQLiteConnection | null
  private readonly readers: SQLiteConnection[]
  private readerIndex = 0
  private closed = false

  private constructor(writer: SQLiteConnection | null, readers: SQLiteConnection[]) {
    this.writer = writer
    this.readers = readers
  }

  static async create(options: ConnectionPoolOptions): Promise<ConnectionPool> {
    const { driver, path, readOnly = false, readPoolSize = 4, walMode = true } = options

    let writer: SQLiteConnection | null = null
    const readers: SQLiteConnection[] = []

    try {
      if (!readOnly) {
        writer = await driver.open(path, { walMode })
      }

      const poolSize = driver.capabilities.multipleConnections ? Math.max(readPoolSize, 1) : 0

      for (let i = 0; i < poolSize; i++) {
        const reader = await driver.open(path, { readonly: true, walMode: false })
        readers.push(reader)
      }
    } catch (err) {
      await closeAllSilently([...readers, writer])
      throw err
    }

    return new ConnectionPool(writer, readers)
  }

  acquireReader(): SQLiteConnection {
    if (this.closed) {
      throw new ConnectionPoolError('Connection pool is closed')
    }
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
    if (this.closed) {
      throw new ConnectionPoolError('Connection pool is closed')
    }
    if (!this.writer) {
      throw new ConnectionPoolError('No write connection available (database is read-only)')
    }
    return this.writer
  }

  get readerCount(): number {
    return this.readers.length
  }

  get isReadOnly(): boolean {
    return this.writer === null
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
