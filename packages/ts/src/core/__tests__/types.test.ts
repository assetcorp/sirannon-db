import { describe, it, expect } from 'vitest'
import {
	SirannonError,
	DatabaseNotFoundError,
	DatabaseAlreadyExistsError,
	ReadOnlyError,
	QueryError,
	TransactionError,
	MigrationError,
	HookDeniedError,
	CDCError,
	BackupError,
	ConnectionPoolError,
	MaxDatabasesError,
	ExtensionError,
} from '../errors.js'
import type {
	Params,
	ExecuteResult,
	ChangeEvent,
	QueryHookContext,
	DatabaseOptions,
	SirannonOptions,
	MigrationFile,
	MigrationResult,
	BackupScheduleOptions,
	SubscriptionBuilder,
	Subscription,
	ServerOptions,
	ClientOptions,
} from '../types.js'

describe('Error classes', () => {
	it('SirannonError has code and message', () => {
		const err = new SirannonError('test', 'TEST_CODE')
		expect(err.message).toBe('test')
		expect(err.code).toBe('TEST_CODE')
		expect(err.name).toBe('SirannonError')
		expect(err).toBeInstanceOf(Error)
	})

	it('DatabaseNotFoundError formats id', () => {
		const err = new DatabaseNotFoundError('my-db')
		expect(err.message).toContain('my-db')
		expect(err.code).toBe('DATABASE_NOT_FOUND')
		expect(err).toBeInstanceOf(SirannonError)
	})

	it('DatabaseAlreadyExistsError formats id', () => {
		const err = new DatabaseAlreadyExistsError('my-db')
		expect(err.message).toContain('my-db')
		expect(err.code).toBe('DATABASE_ALREADY_EXISTS')
	})

	it('ReadOnlyError formats id', () => {
		const err = new ReadOnlyError('my-db')
		expect(err.message).toContain('read-only')
		expect(err.code).toBe('READ_ONLY')
	})

	it('QueryError includes SQL', () => {
		const err = new QueryError('syntax error', 'SELECT * FORM users')
		expect(err.sql).toBe('SELECT * FORM users')
		expect(err.code).toBe('QUERY_ERROR')
	})

	it('TransactionError', () => {
		const err = new TransactionError('rollback')
		expect(err.code).toBe('TRANSACTION_ERROR')
	})

	it('MigrationError includes version', () => {
		const err = new MigrationError('failed', 3)
		expect(err.version).toBe(3)
		expect(err.code).toBe('MIGRATION_ERROR')
	})

	it('HookDeniedError with and without reason', () => {
		const err1 = new HookDeniedError('auth')
		expect(err1.message).toContain('auth')
		expect(err1.code).toBe('HOOK_DENIED')

		const err2 = new HookDeniedError('auth', 'not allowed')
		expect(err2.message).toContain('not allowed')
	})

	it('CDCError', () => {
		const err = new CDCError('polling failed')
		expect(err.code).toBe('CDC_ERROR')
	})

	it('BackupError', () => {
		const err = new BackupError('disk full')
		expect(err.code).toBe('BACKUP_ERROR')
	})

	it('ConnectionPoolError', () => {
		const err = new ConnectionPoolError('exhausted')
		expect(err.code).toBe('CONNECTION_POOL_ERROR')
	})

	it('MaxDatabasesError formats limit', () => {
		const err = new MaxDatabasesError(10)
		expect(err.message).toContain('10')
		expect(err.code).toBe('MAX_DATABASES')
	})

	it('ExtensionError with and without cause', () => {
		const err1 = new ExtensionError('/path/ext.so')
		expect(err1.message).toContain('/path/ext.so')

		const err2 = new ExtensionError('/path/ext.so', 'not found')
		expect(err2.message).toContain('not found')
		expect(err2.code).toBe('EXTENSION_ERROR')
	})
})

describe('Type exports are accessible', () => {
	it('core types are importable', () => {
		// Type-level only checks; this test passes if the module compiles.
		const result: ExecuteResult = { changes: 1, lastInsertRowId: 1 }
		expect(result.changes).toBe(1)

		const event: ChangeEvent = {
			type: 'insert',
			table: 'users',
			row: { id: 1 },
			seq: 1n,
			timestamp: Date.now(),
		}
		expect(event.type).toBe('insert')
	})
})
