import Database from 'better-sqlite3'
import { ConnectionPoolError } from './errors.js'

export interface ConnectionPoolOptions {
  path: string
  readOnly?: boolean
  readPoolSize?: number
  walMode?: boolean
}

type SqliteDb = InstanceType<typeof Database>

function closeAllSilently(connections: (SqliteDb | null)[]): void {
  for (const conn of connections) {
    if (!conn) continue
    try {
      conn.close()
    } catch {
      // Best-effort cleanup; the original error takes priority
    }
  }
}

export class ConnectionPool {
  private readonly writer: SqliteDb | null
  private readonly readers: SqliteDb[]
  private readerIndex = 0
  private closed = false

  constructor(options: ConnectionPoolOptions) {
    const { path, readOnly = false, readPoolSize = 4, walMode = true } = options

    let writer: SqliteDb | null = null
    const readers: SqliteDb[] = []

    try {
      if (!readOnly) {
        writer = new Database(path)
        if (walMode) {
          writer.pragma('journal_mode = WAL')
        }
        writer.pragma('synchronous = NORMAL')
        writer.pragma('foreign_keys = ON')
      }

      const poolSize = Math.max(readPoolSize, 1)
      for (let i = 0; i < poolSize; i++) {
        const reader = new Database(path, { readonly: true })
        readers.push(reader)
        reader.pragma('foreign_keys = ON')
      }
    } catch (err) {
      closeAllSilently([...readers, writer])
      throw err
    }

    this.writer = writer
    this.readers = readers
  }

  acquireReader(): SqliteDb {
    if (this.closed) {
      throw new ConnectionPoolError('Connection pool is closed')
    }
    const reader = this.readers[this.readerIndex % this.readers.length]
    this.readerIndex = (this.readerIndex + 1) % this.readers.length
    return reader
  }

  acquireWriter(): SqliteDb {
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

  close(): void {
    if (this.closed) return
    this.closed = true

    const errors: unknown[] = []

    for (const reader of this.readers) {
      try {
        reader.close()
      } catch (err) {
        errors.push(err)
      }
    }

    if (this.writer) {
      try {
        this.writer.close()
      } catch (err) {
        errors.push(err)
      }
    }

    if (errors.length > 0) {
      throw new ConnectionPoolError(`Failed to close ${errors.length} connection(s)`)
    }
  }
}
