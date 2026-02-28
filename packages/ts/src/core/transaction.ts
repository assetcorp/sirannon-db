import Database from 'better-sqlite3'
import { QueryExecutor } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'

type SqliteDb = InstanceType<typeof Database>

export class Transaction {
	private _lastInsertRowId: number | bigint = 0

	constructor(private readonly db: SqliteDb) {}

	query<T = Record<string, unknown>>(sql: string, params?: Params): T[] {
		return QueryExecutor.query<T>(this.db, sql, params)
	}

	execute(sql: string, params?: Params): ExecuteResult {
		const result = QueryExecutor.execute(this.db, sql, params)
		this._lastInsertRowId = result.lastInsertRowId
		return result
	}

	executeBatch(sql: string, paramsBatch: Params[]): ExecuteResult[] {
		const results = QueryExecutor.executeBatch(this.db, sql, paramsBatch)
		if (results.length > 0) {
			this._lastInsertRowId = results[results.length - 1].lastInsertRowId
		}
		return results
	}

	get lastInsertRowId(): number | bigint {
		return this._lastInsertRowId
	}

	static run<T>(db: SqliteDb, fn: (tx: Transaction) => T): T {
		const tx = new Transaction(db)
		return db.transaction(() => fn(tx))()
	}
}
