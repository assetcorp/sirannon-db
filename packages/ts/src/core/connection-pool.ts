import Database from 'better-sqlite3'
import { ConnectionPoolError } from './errors.js'

export interface ConnectionPoolOptions {
	path: string
	readOnly?: boolean
	readPoolSize?: number
	walMode?: boolean
}

type SqliteDb = InstanceType<typeof Database>

export class ConnectionPool {
	private readonly writer: SqliteDb | null
	private readonly readers: SqliteDb[]
	private readerIndex = 0
	private closed = false

	constructor(options: ConnectionPoolOptions) {
		const { path, readOnly = false, readPoolSize = 4, walMode = true } = options

		if (readOnly) {
			this.writer = null
		} else {
			this.writer = new Database(path)
			if (walMode) {
				this.writer.pragma('journal_mode = WAL')
			}
			this.writer.pragma('synchronous = NORMAL')
			this.writer.pragma('foreign_keys = ON')
		}

		this.readers = []
		const poolSize = Math.max(readPoolSize, 1)
		for (let i = 0; i < poolSize; i++) {
			const reader = new Database(path, { readonly: true })
			reader.pragma('foreign_keys = ON')
			this.readers.push(reader)
		}
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
			throw new ConnectionPoolError(
				'No write connection available (database is read-only)',
			)
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
		for (const reader of this.readers) {
			reader.close()
		}
		this.writer?.close()
	}
}
