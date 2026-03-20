import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ConnectionPool } from '../connection-pool.js'
import { ConnectionPoolError } from '../errors.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-pool-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

async function seedDatabase(path: string) {
  const conn = await testDriver.open(path)
  await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await conn.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
  await conn.close()
}

describe('ConnectionPool', () => {
  it('creates the default number of readers (4) and one writer', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    expect(pool.readerCount).toBe(4)
    expect(pool.isReadOnly).toBe(false)
    await pool.close()
  })

  it('respects a custom readPoolSize', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: dbPath,
      readPoolSize: 2,
    })
    expect(pool.readerCount).toBe(2)
    await pool.close()
  })

  it('enforces at least one reader', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: dbPath,
      readPoolSize: 0,
    })
    expect(pool.readerCount).toBe(1)
    await pool.close()
  })

  it('distributes readers round-robin', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({
      driver: testDriver,
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
    await pool.close()
  })

  it('returns a single writer connection', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })

    const w1 = pool.acquireWriter()
    const w2 = pool.acquireWriter()
    expect(w1).toBe(w2)
    await pool.close()
  })

  it('enables WAL mode by default', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })

    const writer = pool.acquireWriter()
    const stmt = await writer.prepare('PRAGMA journal_mode')
    const row = await stmt.get<{ journal_mode: string }>()
    expect(row?.journal_mode).toBe('wal')
    await pool.close()
  })

  it('disables WAL mode when walMode is false', async () => {
    const dbPath = join(tempDir, 'nowal.db')
    const setupConn = await testDriver.open(dbPath, { walMode: false })
    await setupConn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await setupConn.close()

    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: dbPath,
      walMode: false,
    })

    const writer = pool.acquireWriter()
    const stmt = await writer.prepare('PRAGMA journal_mode')
    const row = await stmt.get<{ journal_mode: string }>()
    expect(row?.journal_mode).not.toBe('wal')
    await pool.close()
  })

  it('creates a read-only pool without a writer', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: dbPath,
      readOnly: true,
    })

    expect(pool.isReadOnly).toBe(true)
    expect(() => pool.acquireWriter()).toThrow(ConnectionPoolError)
    await pool.close()
  })

  it('read-only readers can execute SELECT', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: dbPath,
      readOnly: true,
    })

    const reader = pool.acquireReader()
    const stmt = await reader.prepare('SELECT * FROM users')
    const rows = await stmt.all()
    expect(rows).toHaveLength(2)
    await pool.close()
  })

  it('throws ConnectionPoolError on acquireReader after close', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    await pool.close()

    expect(() => pool.acquireReader()).toThrow(ConnectionPoolError)
  })

  it('throws ConnectionPoolError on acquireWriter after close', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    await pool.close()

    expect(() => pool.acquireWriter()).toThrow(ConnectionPoolError)
  })

  it('close is idempotent', async () => {
    const dbPath = join(tempDir, 'test.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    await pool.close()
    await expect(pool.close()).resolves.not.toThrow()
  })

  it('throws on non-existent directory path', async () => {
    const badPath = join(tempDir, 'no', 'such', 'dir', 'test.db')
    await expect(ConnectionPool.create({ driver: testDriver, path: badPath })).rejects.toThrow()
  })

  it('aggregates close errors from readers and writer', async () => {
    const dbPath = join(tempDir, 'close-errors.db')
    await seedDatabase(dbPath)
    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath, readPoolSize: 2 })

    const internals = pool as unknown as {
      readers: { close: () => Promise<void> }[]
      writer: { close: () => Promise<void> } | null
    }

    for (const reader of internals.readers) {
      reader.close = async () => {
        throw new Error('reader close failure')
      }
    }
    if (internals.writer) {
      internals.writer.close = async () => {
        throw new Error('writer close failure')
      }
    }

    await expect(pool.close()).rejects.toThrow(ConnectionPoolError)
  })

  it('reopens on same path after close', async () => {
    const dbPath = join(tempDir, 'partial.db')
    await seedDatabase(dbPath)

    const pool = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    await pool.close()

    const pool2 = await ConnectionPool.create({ driver: testDriver, path: dbPath })
    const writer = pool2.acquireWriter()
    const stmt = await writer.prepare('SELECT * FROM users')
    const rows = await stmt.all()
    expect(rows).toHaveLength(2)
    await pool2.close()
  })
})
