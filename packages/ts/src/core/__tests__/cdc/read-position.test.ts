import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { decodeReadPosition, encodeReadPosition } from '../../cdc/read-position.js'
import type { Database } from '../../database.js'
import type { DatabaseCdcController } from '../../database-cdc.js'
import type { OpenOptions, SQLiteConnection, SQLiteDriver } from '../../driver/types.js'
import { query } from '../../query-executor.js'
import { Sirannon } from '../../sirannon.js'

let tempDir: string
let sirannon: Sirannon
let db: Database

async function readAt<T = Record<string, unknown>>(target: Database, sql: string) {
  const cdc = (target as unknown as { runtime: { cdc: DatabaseCdcController } }).runtime.cdc
  const captured = await cdc.readAtPositionWith(conn => query<T>(conn, sql))
  return { rows: captured.value, position: captured.position }
}

interface ConnectionLedger {
  driver: SQLiteDriver
  opened: number
  closed: number
}

function countingDriver(inner: SQLiteDriver): ConnectionLedger {
  const ledger = { opened: 0, closed: 0 } as { opened: number; closed: number }
  const driver: SQLiteDriver = {
    ...inner,
    async open(path: string, options?: OpenOptions): Promise<SQLiteConnection> {
      const conn = await inner.open(path, options)
      ledger.opened++
      return {
        ...conn,
        exec: sql => conn.exec(sql),
        prepare: sql => conn.prepare(sql),
        transaction: fn => conn.transaction(fn),
        async close() {
          ledger.closed++
          await conn.close()
        },
      }
    },
  }
  return {
    driver,
    get opened() {
      return ledger.opened
    },
    get closed() {
      return ledger.closed
    },
  } as ConnectionLedger
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-read-position-'))
  sirannon = new Sirannon({ driver: betterSqlite3() })
  db = await sirannon.open('shop', join(tempDir, 'shop.db'))
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
  await db.watch('orders')
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('read position tokens', () => {
  it('round-trips an epoch and a sequence', () => {
    const token = encodeReadPosition({ epoch: 'a1b2c3', seq: 9007199254740993n })
    expect(token).not.toContain('9007199254740993')
    expect(decodeReadPosition(token)).toEqual({ epoch: 'a1b2c3', seq: 9007199254740993n })
  })

  it('refuses a token it did not issue', () => {
    expect(decodeReadPosition('')).toBeNull()
    expect(decodeReadPosition('not-hex')).toBeNull()
    expect(decodeReadPosition('abc')).toBeNull()
    expect(decodeReadPosition(encodeReadPosition({ epoch: 'ab', seq: 1n }).slice(0, -2))).toBeNull()
  })

  it('refuses to issue a token for an epoch that is not an epoch', () => {
    expect(() => encodeReadPosition({ epoch: 'not:an:epoch', seq: 1n })).toThrow('lower-case hex')
  })
})

describe('positioned reads', () => {
  it('returns the rows and a position the same subscription cursor understands', async () => {
    await db.execute('INSERT INTO orders (id, total) VALUES (1, 100)')

    const result = await readAt<{ id: number; total: number }>(db, 'SELECT * FROM orders ORDER BY id')

    expect(result.rows).toEqual([{ id: 1, total: 100 }])
    const decoded = decodeReadPosition(result.position)
    expect(decoded).not.toBeNull()
    expect(decoded?.seq).toBeGreaterThan(0n)
  })

  it('carries the identity of the file that issued it', async () => {
    const other = await sirannon.open('warehouse', join(tempDir, 'warehouse.db'))
    await other.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await other.watch('orders')
    await other.execute('INSERT INTO orders (id) VALUES (1)')
    await db.execute('INSERT INTO orders (id, total) VALUES (1, 100)')

    const here = decodeReadPosition((await readAt(db, 'SELECT * FROM orders')).position)
    const there = decodeReadPosition((await readAt(other, 'SELECT * FROM orders')).position)

    expect(here?.epoch).toBeDefined()
    expect(there?.epoch).toBeDefined()
    expect(here?.epoch).not.toBe(there?.epoch)
  })

  it('advances the position exactly as far as the rows it returned', async () => {
    await db.execute('INSERT INTO orders (id, total) VALUES (1, 100)')
    const first = await readAt<{ id: number }>(db, 'SELECT * FROM orders ORDER BY id')

    await db.execute('INSERT INTO orders (id, total) VALUES (2, 200)')
    const second = await readAt<{ id: number }>(db, 'SELECT * FROM orders ORDER BY id')

    const firstSeq = decodeReadPosition(first.position)?.seq ?? 0n
    const secondSeq = decodeReadPosition(second.position)?.seq ?? 0n

    expect(first.rows).toHaveLength(1)
    expect(second.rows).toHaveLength(2)
    expect(secondSeq).toBeGreaterThan(firstSeq)
  })

  it('reports a position on a database whose change log is still empty', async () => {
    const fresh = await sirannon.open('empty', join(tempDir, 'empty.db'))
    await fresh.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')

    const result = await readAt(fresh, 'SELECT * FROM orders')

    expect(result.rows).toEqual([])
    expect(decodeReadPosition(result.position)?.seq).toBe(0n)
  })

  it('refuses an internal table through the same guard as query', async () => {
    await expect(readAt(db, 'SELECT * FROM _sirannon_changes')).rejects.toThrow(
      'Access to internal tables is not permitted',
    )
  })

  it('runs writes and other reads while the positioned read is open', async () => {
    await db.execute('INSERT INTO orders (id, total) VALUES (1, 100)')

    const [positioned, plain, written] = await Promise.all([
      readAt<{ id: number }>(db, 'SELECT * FROM orders ORDER BY id'),
      db.query<{ id: number }>('SELECT * FROM orders ORDER BY id'),
      db.execute('INSERT INTO orders (id, total) VALUES (2, 200)'),
    ])

    expect(positioned.rows.length).toBeGreaterThanOrEqual(1)
    expect(plain.length).toBeGreaterThanOrEqual(1)
    expect(written.changes).toBe(1)
    expect(await db.query('SELECT * FROM orders')).toHaveLength(2)
  })
})

describe('positioned read isolation', () => {
  it('leaves a write that commits mid-read out of both the rows and the position', async () => {
    let afterNextRead: (() => Promise<void>) | null = null
    const inner = betterSqlite3()
    const driver: SQLiteDriver = {
      ...inner,
      async open(path: string, options?: OpenOptions): Promise<SQLiteConnection> {
        const conn = await inner.open(path, options)
        const wrapped: SQLiteConnection = {
          ...conn,
          exec: sql => conn.exec(sql),
          transaction: fn => conn.transaction(() => fn(wrapped)),
          close: () => conn.close(),
          async prepare(sql: string) {
            const stmt = await conn.prepare(sql)
            return {
              ...stmt,
              get: (...params: unknown[]) => stmt.get(...params),
              run: (...params: unknown[]) => stmt.run(...params),
              async all<T>(...params: unknown[]): Promise<T[]> {
                const rows = await stmt.all<T>(...params)
                const hook = afterNextRead
                afterNextRead = null
                if (hook) await hook()
                return rows
              },
            }
          },
        }
        return wrapped
      },
    }

    const isolated = new Sirannon({ driver })
    const scoped = await isolated.open('shop', join(tempDir, 'isolation.db'))
    await scoped.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
    await scoped.watch('orders')
    await scoped.execute('INSERT INTO orders (id, total) VALUES (1, 100)')

    afterNextRead = async () => {
      await scoped.execute('INSERT INTO orders (id, total) VALUES (2, 200)')
    }
    const snapshot = await readAt<{ id: number }>(scoped, 'SELECT * FROM orders ORDER BY id')
    const later = await readAt<{ id: number }>(scoped, 'SELECT * FROM orders ORDER BY id')

    expect(snapshot.rows.map(row => row.id)).toEqual([1])
    expect(later.rows.map(row => row.id)).toEqual([1, 2])

    const snapshotSeq = decodeReadPosition(snapshot.position)?.seq ?? 0n
    const laterSeq = decodeReadPosition(later.position)?.seq ?? 0n
    expect(snapshotSeq).toBeLessThan(laterSeq)

    await isolated.shutdown()
  })
})

describe('positioned read connection handling', () => {
  it('closes its connection whether the read succeeds or fails', async () => {
    const ledger = countingDriver(betterSqlite3())
    const isolated = new Sirannon({ driver: ledger.driver })
    const scoped = await isolated.open('shop', join(tempDir, 'ledger.db'))
    await scoped.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await scoped.watch('orders')

    const openedBefore = ledger.opened
    const closedBefore = ledger.closed

    await readAt(scoped, 'SELECT * FROM orders')
    await expect(readAt(scoped, 'SELECT * FROM nope')).rejects.toThrow()

    expect(ledger.opened - openedBefore).toBe(2)
    expect(ledger.closed - closedBefore).toBe(2)

    await isolated.shutdown()
  })
})
