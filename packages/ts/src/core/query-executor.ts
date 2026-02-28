import Database from 'better-sqlite3'
import { QueryError } from './errors.js'
import type { ExecuteResult, Params } from './types.js'

type SqliteDb = InstanceType<typeof Database>

function bindParams(params?: Params): unknown[] {
	if (params === undefined) return []
	if (Array.isArray(params)) return params
	return [params]
}

export class QueryExecutor {
	static query<T = Record<string, unknown>>(
		db: SqliteDb,
		sql: string,
		params?: Params,
	): T[] {
		try {
			const stmt = db.prepare(sql)
			return stmt.all(...bindParams(params)) as T[]
		} catch (err) {
			throw new QueryError(
				err instanceof Error ? err.message : String(err),
				sql,
			)
		}
	}

	static queryOne<T = Record<string, unknown>>(
		db: SqliteDb,
		sql: string,
		params?: Params,
	): T | undefined {
		try {
			const stmt = db.prepare(sql)
			return stmt.get(...bindParams(params)) as T | undefined
		} catch (err) {
			throw new QueryError(
				err instanceof Error ? err.message : String(err),
				sql,
			)
		}
	}

	static execute(db: SqliteDb, sql: string, params?: Params): ExecuteResult {
		try {
			const stmt = db.prepare(sql)
			const result = stmt.run(...bindParams(params))
			return {
				changes: result.changes,
				lastInsertRowId: result.lastInsertRowid,
			}
		} catch (err) {
			throw new QueryError(
				err instanceof Error ? err.message : String(err),
				sql,
			)
		}
	}

	static executeBatch(
		db: SqliteDb,
		sql: string,
		paramsBatch: Params[],
	): ExecuteResult[] {
		try {
			const stmt = db.prepare(sql)
			const results: ExecuteResult[] = []
			for (const params of paramsBatch) {
				const result = stmt.run(...bindParams(params))
				results.push({
					changes: result.changes,
					lastInsertRowId: result.lastInsertRowid,
				})
			}
			return results
		} catch (err) {
			throw new QueryError(
				err instanceof Error ? err.message : String(err),
				sql,
			)
		}
	}
}
