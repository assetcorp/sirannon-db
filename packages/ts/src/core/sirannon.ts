import { Database } from './database.js'
import {
	DatabaseAlreadyExistsError,
	DatabaseNotFoundError,
	SirannonError,
} from './errors.js'
import type {
	AfterQueryHook,
	BeforeConnectHook,
	BeforeQueryHook,
	DatabaseCloseHook,
	DatabaseOpenHook,
	DatabaseOptions,
	SirannonOptions,
} from './types.js'

export class Sirannon {
	private readonly dbs = new Map<string, Database>()
	private _shutdown = false

	constructor(private readonly options?: SirannonOptions) {}

	open(id: string, path: string, options?: DatabaseOptions): Database {
		this.ensureRunning()
		if (this.dbs.has(id)) {
			throw new DatabaseAlreadyExistsError(id)
		}
		const db = new Database(id, path, options)
		this.dbs.set(id, db)
		return db
	}

	close(id: string): void {
		const db = this.dbs.get(id)
		if (!db) {
			throw new DatabaseNotFoundError(id)
		}
		db.close()
		this.dbs.delete(id)
	}

	get(id: string): Database | undefined {
		return this.dbs.get(id)
	}

	has(id: string): boolean {
		return this.dbs.has(id)
	}

	databases(): Map<string, Database> {
		return new Map(this.dbs)
	}

	shutdown(): void {
		if (this._shutdown) return
		this._shutdown = true
		for (const [, db] of this.dbs) {
			db.close()
		}
		this.dbs.clear()
	}

	onBeforeQuery(_hook: BeforeQueryHook): void {
		throw new Error('not implemented')
	}

	onAfterQuery(_hook: AfterQueryHook): void {
		throw new Error('not implemented')
	}

	onBeforeConnect(_hook: BeforeConnectHook): void {
		throw new Error('not implemented')
	}

	onDatabaseOpen(_hook: DatabaseOpenHook): void {
		throw new Error('not implemented')
	}

	onDatabaseClose(_hook: DatabaseCloseHook): void {
		throw new Error('not implemented')
	}

	private ensureRunning(): void {
		if (this._shutdown) {
			throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
		}
	}
}
