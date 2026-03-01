import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
	DatabaseAlreadyExistsError,
	DatabaseNotFoundError,
	SirannonError,
} from '../errors.js'
import { Sirannon } from '../sirannon.js'

let tempDir: string

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), 'sirannon-reg-'))
})

afterEach(() => {
	rmSync(tempDir, { recursive: true, force: true })
})

describe('Sirannon', () => {
	it('opens a database and returns it', () => {
		const sir = new Sirannon()
		const db = sir.open('main', join(tempDir, 'main.db'))
		expect(db.id).toBe('main')
		expect(db.closed).toBe(false)
		sir.shutdown()
	})

	it('throws DatabaseAlreadyExistsError on duplicate id', () => {
		const sir = new Sirannon()
		sir.open('main', join(tempDir, 'main.db'))

		expect(() => sir.open('main', join(tempDir, 'other.db'))).toThrow(
			DatabaseAlreadyExistsError,
		)
		sir.shutdown()
	})

	it('closes a database by id', () => {
		const sir = new Sirannon()
		const db = sir.open('main', join(tempDir, 'main.db'))
		sir.close('main')
		expect(db.closed).toBe(true)
		expect(sir.has('main')).toBe(false)
		sir.shutdown()
	})

	it('throws DatabaseNotFoundError when closing an unknown id', () => {
		const sir = new Sirannon()
		expect(() => sir.close('nope')).toThrow(DatabaseNotFoundError)
		sir.shutdown()
	})

	it('gets a database by id', () => {
		const sir = new Sirannon()
		const db = sir.open('main', join(tempDir, 'main.db'))
		expect(sir.get('main')).toBe(db)
		expect(sir.get('nope')).toBeUndefined()
		sir.shutdown()
	})

	it('checks whether a database exists', () => {
		const sir = new Sirannon()
		expect(sir.has('main')).toBe(false)
		sir.open('main', join(tempDir, 'main.db'))
		expect(sir.has('main')).toBe(true)
		sir.shutdown()
	})

	it('returns a copy of the databases map', () => {
		const sir = new Sirannon()
		sir.open('a', join(tempDir, 'a.db'))
		sir.open('b', join(tempDir, 'b.db'))

		const dbs = sir.databases()
		expect(dbs.size).toBe(2)
		expect(dbs.has('a')).toBe(true)
		expect(dbs.has('b')).toBe(true)

		dbs.delete('a')
		expect(sir.has('a')).toBe(true)
		sir.shutdown()
	})

	it('manages multiple databases independently', () => {
		const sir = new Sirannon()
		const db1 = sir.open('users', join(tempDir, 'users.db'))
		const db2 = sir.open('products', join(tempDir, 'products.db'))

		db1.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
		db1.execute("INSERT INTO users (name) VALUES ('Alice')")

		db2.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
		db2.execute("INSERT INTO products (title) VALUES ('Widget')")

		expect(db1.query<{ name: string }>('SELECT * FROM users')).toHaveLength(1)
		expect(db2.query<{ title: string }>('SELECT * FROM products')).toHaveLength(
			1,
		)
		sir.shutdown()
	})

	it('shutdown closes all databases', () => {
		const sir = new Sirannon()
		const db1 = sir.open('a', join(tempDir, 'a.db'))
		const db2 = sir.open('b', join(tempDir, 'b.db'))

		sir.shutdown()
		expect(db1.closed).toBe(true)
		expect(db2.closed).toBe(true)
	})

	it('shutdown is idempotent', () => {
		const sir = new Sirannon()
		sir.open('a', join(tempDir, 'a.db'))
		sir.shutdown()
		expect(() => sir.shutdown()).not.toThrow()
	})

	it('throws after shutdown on open', () => {
		const sir = new Sirannon()
		sir.shutdown()
		expect(() => sir.open('main', join(tempDir, 'main.db'))).toThrow(
			SirannonError,
		)
	})

	it('throws after shutdown on close', () => {
		const sir = new Sirannon()
		sir.shutdown()
		expect(() => sir.close('main')).toThrow(SirannonError)
	})

	it('passes database options through to open', () => {
		const sir = new Sirannon()
		const db = sir.open('ro', join(tempDir, 'ro.db'), {
			readOnly: false,
			readPoolSize: 2,
		})
		expect(db.readerCount).toBe(2)
		sir.shutdown()
	})

	it('wraps open errors in SirannonError with DATABASE_OPEN_FAILED', () => {
		const sir = new Sirannon()
		const badPath = join(tempDir, 'no', 'such', 'dir', 'test.db')

		try {
			sir.open('bad', badPath)
			expect.fail('should have thrown')
		} catch (err) {
			expect(err).toBeInstanceOf(SirannonError)
			const sErr = err as SirannonError
			expect(sErr.code).toBe('DATABASE_OPEN_FAILED')
			expect(sErr.message).toContain('bad')
			expect(sErr.message).toContain(badPath)
		}

		sir.shutdown()
	})

	it('removes database from registry when closed directly', () => {
		const sir = new Sirannon()
		const db = sir.open('main', join(tempDir, 'main.db'))

		db.close()

		expect(sir.has('main')).toBe(false)
		expect(sir.get('main')).toBeUndefined()
		expect(sir.databases().size).toBe(0)
		sir.shutdown()
	})

	it('allows re-opening after direct close', () => {
		const sir = new Sirannon()
		const db1 = sir.open('main', join(tempDir, 'main.db'))
		db1.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
		db1.execute("INSERT INTO notes (body) VALUES ('hello')")
		db1.close()

		const db2 = sir.open('main', join(tempDir, 'main.db'))
		const rows = db2.query<{ body: string }>('SELECT * FROM notes')
		expect(rows).toHaveLength(1)
		expect(rows[0].body).toBe('hello')
		sir.shutdown()
	})

	it('sir.close on a directly-closed db throws DatabaseNotFoundError', () => {
		const sir = new Sirannon()
		const db = sir.open('main', join(tempDir, 'main.db'))
		db.close()

		expect(() => sir.close('main')).toThrow(DatabaseNotFoundError)
		sir.shutdown()
	})
})
