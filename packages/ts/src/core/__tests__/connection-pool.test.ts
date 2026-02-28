import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ConnectionPool } from '../connection-pool.js'
import { ConnectionPoolError } from '../errors.js'

let tempDir: string

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), 'sirannon-pool-'))
})

afterEach(() => {
	rmSync(tempDir, { recursive: true, force: true })
})

function seedDatabase(path: string) {
	const db = new Database(path)
	db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
	db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
	db.close()
}

describe('ConnectionPool', () => {
	it('creates the default number of readers (4) and one writer', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })
		expect(pool.readerCount).toBe(4)
		expect(pool.isReadOnly).toBe(false)
		pool.close()
	})

	it('respects a custom readPoolSize', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			readPoolSize: 2,
		})
		expect(pool.readerCount).toBe(2)
		pool.close()
	})

	it('enforces at least one reader', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			readPoolSize: 0,
		})
		expect(pool.readerCount).toBe(1)
		pool.close()
	})

	it('distributes readers round-robin', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			readPoolSize: 3,
		})

		const first = pool.acquireReader()
		const second = pool.acquireReader()
		const third = pool.acquireReader()
		const fourth = pool.acquireReader()

		expect(first).not.toBe(second)
		expect(second).not.toBe(third)
		expect(fourth).toBe(first)
		pool.close()
	})

	it('returns a single writer connection', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })

		const w1 = pool.acquireWriter()
		const w2 = pool.acquireWriter()
		expect(w1).toBe(w2)
		pool.close()
	})

	it('enables WAL mode by default', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })

		const writer = pool.acquireWriter()
		const mode = writer.pragma('journal_mode', { simple: true })
		expect(mode).toBe('wal')
		pool.close()
	})

	it('disables WAL mode when walMode is false', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			walMode: false,
		})

		const writer = pool.acquireWriter()
		const mode = writer.pragma('journal_mode', { simple: true })
		expect(mode).not.toBe('wal')
		pool.close()
	})

	it('creates a read-only pool without a writer', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			readOnly: true,
		})

		expect(pool.isReadOnly).toBe(true)
		expect(() => pool.acquireWriter()).toThrow(ConnectionPoolError)
		pool.close()
	})

	it('read-only readers can execute SELECT', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({
			path: dbPath,
			readOnly: true,
		})

		const reader = pool.acquireReader()
		const rows = reader.prepare('SELECT * FROM users').all()
		expect(rows).toHaveLength(2)
		pool.close()
	})

	it('throws ConnectionPoolError on acquireReader after close', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })
		pool.close()

		expect(() => pool.acquireReader()).toThrow(ConnectionPoolError)
	})

	it('throws ConnectionPoolError on acquireWriter after close', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })
		pool.close()

		expect(() => pool.acquireWriter()).toThrow(ConnectionPoolError)
	})

	it('close is idempotent', () => {
		const dbPath = join(tempDir, 'test.db')
		seedDatabase(dbPath)
		const pool = new ConnectionPool({ path: dbPath })
		pool.close()
		expect(() => pool.close()).not.toThrow()
	})
})
