import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DatabaseAlreadyExistsError, DatabaseNotFoundError, SirannonError } from '../errors.js'
import { Sirannon } from '../sirannon.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-reg-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Sirannon', () => {
  it('opens a database and returns it', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))
    expect(db.id).toBe('main')
    expect(db.closed).toBe(false)
    await sir.shutdown()
  })

  it('throws DatabaseAlreadyExistsError on duplicate id', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await sir.open('main', join(tempDir, 'main.db'))

    await expect(sir.open('main', join(tempDir, 'other.db'))).rejects.toThrow(DatabaseAlreadyExistsError)
    await sir.shutdown()
  })

  it('closes a database by id', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))
    await sir.close('main')
    expect(db.closed).toBe(true)
    expect(sir.has('main')).toBe(false)
    await sir.shutdown()
  })

  it('throws DatabaseNotFoundError when closing an unknown id', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await expect(sir.close('nope')).rejects.toThrow(DatabaseNotFoundError)
    await sir.shutdown()
  })

  it('gets a database by id', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))
    expect(sir.get('main')).toBe(db)
    expect(sir.get('nope')).toBeUndefined()
    await sir.shutdown()
  })

  it('checks whether a database exists', async () => {
    const sir = new Sirannon({ driver: testDriver })
    expect(sir.has('main')).toBe(false)
    await sir.open('main', join(tempDir, 'main.db'))
    expect(sir.has('main')).toBe(true)
    await sir.shutdown()
  })

  it('returns a copy of the databases map', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await sir.open('a', join(tempDir, 'a.db'))
    await sir.open('b', join(tempDir, 'b.db'))

    const dbs = sir.databases()
    expect(dbs.size).toBe(2)
    expect(dbs.has('a')).toBe(true)
    expect(dbs.has('b')).toBe(true)

    dbs.delete('a')
    expect(sir.has('a')).toBe(true)
    await sir.shutdown()
  })

  it('manages multiple databases independently', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db1 = await sir.open('users', join(tempDir, 'users.db'))
    const db2 = await sir.open('products', join(tempDir, 'products.db'))

    await db1.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db1.execute("INSERT INTO users (name) VALUES ('Alice')")

    await db2.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
    await db2.execute("INSERT INTO products (title) VALUES ('Widget')")

    expect(await db1.query<{ name: string }>('SELECT * FROM users')).toHaveLength(1)
    expect(await db2.query<{ title: string }>('SELECT * FROM products')).toHaveLength(1)
    await sir.shutdown()
  })

  it('shutdown closes all databases', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db1 = await sir.open('a', join(tempDir, 'a.db'))
    const db2 = await sir.open('b', join(tempDir, 'b.db'))

    await sir.shutdown()
    expect(db1.closed).toBe(true)
    expect(db2.closed).toBe(true)
  })

  it('shutdown is idempotent', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await sir.open('a', join(tempDir, 'a.db'))
    await sir.shutdown()
    await expect(sir.shutdown()).resolves.not.toThrow()
  })

  it('throws after shutdown on open', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await sir.shutdown()
    await expect(sir.open('main', join(tempDir, 'main.db'))).rejects.toThrow(SirannonError)
  })

  it('throws after shutdown on close', async () => {
    const sir = new Sirannon({ driver: testDriver })
    await sir.shutdown()
    await expect(sir.close('main')).rejects.toThrow(SirannonError)
  })

  it('passes database options through to open', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('ro', join(tempDir, 'ro.db'), {
      readOnly: false,
      readPoolSize: 2,
    })
    expect(db.readerCount).toBe(2)
    await sir.shutdown()
  })

  it('wraps open errors in SirannonError with DATABASE_OPEN_FAILED', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const badPath = join(tempDir, 'no', 'such', 'dir', 'test.db')

    try {
      await sir.open('bad', badPath)
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(SirannonError)
      const sErr = err as SirannonError
      expect(sErr.code).toBe('DATABASE_OPEN_FAILED')
      expect(sErr.message).toContain('bad')
      expect(sErr.message).toContain(badPath)
    }

    await sir.shutdown()
  })

  it('removes database from registry when closed directly', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))

    await db.close()

    expect(sir.has('main')).toBe(false)
    expect(sir.get('main')).toBeUndefined()
    expect(sir.databases().size).toBe(0)
    await sir.shutdown()
  })

  it('allows re-opening after direct close', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db1 = await sir.open('main', join(tempDir, 'main.db'))
    await db1.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db1.execute("INSERT INTO notes (body) VALUES ('hello')")
    await db1.close()

    const db2 = await sir.open('main', join(tempDir, 'main.db'))
    const rows = await db2.query<{ body: string }>('SELECT * FROM notes')
    expect(rows).toHaveLength(1)
    expect(rows[0].body).toBe('hello')
    await sir.shutdown()
  })

  it('sir.close on a directly-closed db throws DatabaseNotFoundError', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))
    await db.close()

    await expect(sir.close('main')).rejects.toThrow(DatabaseNotFoundError)
    await sir.shutdown()
  })

  it('registers hook helpers and invokes them through database lifecycle', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const beforeConnect = vi.fn()
    const databaseOpen = vi.fn()
    const databaseClose = vi.fn()
    const afterQuery = vi.fn()

    sir.onBeforeConnect(beforeConnect)
    sir.onDatabaseOpen(databaseOpen)
    sir.onDatabaseClose(databaseClose)
    sir.onAfterQuery(afterQuery)

    const db = await sir.open('main', join(tempDir, 'hooks.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.query('SELECT * FROM users')
    await db.close()

    expect(beforeConnect).toHaveBeenCalledOnce()
    expect(databaseOpen).toHaveBeenCalledOnce()
    expect(databaseClose).toHaveBeenCalledOnce()
    expect(afterQuery).toHaveBeenCalled()
    await sir.shutdown()
  })

  it('throws SHUTDOWN_ERROR when closing a database fails during shutdown', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'shutdown-error.db'))
    ;(db as unknown as { close: () => Promise<void> }).close = async () => {
      throw new Error('close failure')
    }

    try {
      await sir.shutdown()
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(SirannonError)
      expect((err as SirannonError).code).toBe('SHUTDOWN_ERROR')
    }
  })

  it('rethrows SirannonError from Database.create', async () => {
    vi.resetModules()

    try {
      const { SirannonError: MockSirannonError } = await import('../errors.js')
      const ctorError = new MockSirannonError('constructor failed', 'DATABASE_CLOSED')
      vi.doMock('../database.js', () => ({
        Database: {
          create: async () => {
            throw ctorError
          },
        },
      }))

      const { Sirannon: MockSirannon } = await import('../sirannon.js')
      const sir = new MockSirannon({ driver: testDriver })
      await expect(sir.open('main', join(tempDir, 'mocked.db'))).rejects.toThrow(MockSirannonError)
    } finally {
      vi.doUnmock('../database.js')
      vi.resetModules()
    }
  })

  it('wraps non-Error Database.create failures', async () => {
    vi.resetModules()

    try {
      vi.doMock('../database.js', () => ({
        Database: {
          create: async () => {
            throw 'string constructor failure'
          },
        },
      }))

      const { SirannonError: MockSirannonError } = await import('../errors.js')
      const { Sirannon: MockSirannon } = await import('../sirannon.js')
      const sir = new MockSirannon({ driver: testDriver })

      try {
        await sir.open('main', join(tempDir, 'mocked.db'))
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MockSirannonError)
        const sErr = err as InstanceType<typeof MockSirannonError>
        expect(sErr.code).toBe('DATABASE_OPEN_FAILED')
        expect(sErr.message).toContain('string constructor failure')
      }
    } finally {
      vi.doUnmock('../database.js')
      vi.resetModules()
    }
  })
})
