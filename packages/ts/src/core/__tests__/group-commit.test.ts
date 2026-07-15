import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { defineDriver } from '../driver/define.js'
import type { SQLiteDriver } from '../driver/types.js'
import { Sirannon } from '../sirannon.js'

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-group-commit-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

function beginCountingDriver(counter: { begins: number }): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: base.capabilities,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const original = conn.exec.bind(conn)
      ;(conn as { exec: (sql: string) => Promise<void> }).exec = async (sql: string) => {
        if (/^\s*BEGIN/i.test(sql)) counter.begins++
        await original(sql)
      }
      return conn
    },
  })
}

describe('group commit', () => {
  it('commits many concurrent writes under a single transaction', async () => {
    const counter = { begins: 0 }
    const sirannon = new Sirannon({ driver: beginCountingDriver(counter) })
    const db = await sirannon.open('main', join(dir, 'g.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER)')

    const concurrent = 200
    await Promise.all(Array.from({ length: concurrent }, (_, i) => db.execute('INSERT INTO items (n) VALUES (?)', [i])))

    const [{ c }] = await db.query<{ c: number }>('SELECT COUNT(*) AS c FROM items')
    expect(c).toBe(concurrent)
    expect(counter.begins).toBeLessThanOrEqual(3)

    await sirannon.shutdown()
  })

  it('fails only the offending write in a group, committing its neighbours', async () => {
    const sirannon = new Sirannon({ driver: betterSqlite3() })
    const db = await sirannon.open('main', join(dir, 'iso.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)')
    await db.execute('INSERT INTO t (v) VALUES (?)', ['a'])

    const results = await Promise.allSettled([
      db.execute('INSERT INTO t (v) VALUES (?)', ['b']),
      db.execute('INSERT INTO t (v) VALUES (?)', ['a']),
      db.execute('INSERT INTO t (v) VALUES (?)', ['c']),
    ])

    expect(results.map(r => r.status)).toEqual(['fulfilled', 'rejected', 'fulfilled'])
    const rows = await db.query<{ v: string }>('SELECT v FROM t ORDER BY v')
    expect(rows.map(r => r.v)).toEqual(['a', 'b', 'c'])

    await sirannon.shutdown()
  })

  it('groups and isolates correctly through the writer worker', async () => {
    const sirannon = new Sirannon({ driver: betterSqlite3() })
    const db = await sirannon.open('main', join(dir, 'w.db'), { synchronous: 'full', writerWorker: true })
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)')
    await db.execute('INSERT INTO t (v) VALUES (?)', ['seed'])

    const many = await Promise.all(
      Array.from({ length: 100 }, (_, i) => db.execute('INSERT INTO t (v) VALUES (?)', [`row-${i}`])),
    )
    expect(many.every(r => r.changes === 1)).toBe(true)

    const mixed = await Promise.allSettled([
      db.execute('INSERT INTO t (v) VALUES (?)', ['x']),
      db.execute('INSERT INTO t (v) VALUES (?)', ['seed']),
      db.execute('INSERT INTO t (v) VALUES (?)', ['y']),
    ])
    expect(mixed.map(r => r.status)).toEqual(['fulfilled', 'rejected', 'fulfilled'])

    const [{ c }] = await db.query<{ c: number }>('SELECT COUNT(*) AS c FROM t')
    expect(c).toBe(1 + 100 + 2)

    await sirannon.shutdown()
  })
})

function orderStatements(ref: string): { sql: string; params?: unknown[] }[] {
  return [
    { sql: 'INSERT INTO orders (ref) VALUES (?)', params: [ref] },
    { sql: 'INSERT INTO lines (ref, qty) VALUES (?, ?)', params: [ref, 2] },
    { sql: 'UPDATE orders SET total = total + 1 WHERE ref = ?', params: [ref] },
  ]
}

describe('group commit for transactions', () => {
  it('commits many concurrent transactions under a single transaction', async () => {
    const counter = { begins: 0 }
    const sirannon = new Sirannon({ driver: beginCountingDriver(counter) })
    const db = await sirannon.open('main', join(dir, 'gt.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE orders (ref TEXT PRIMARY KEY, total INTEGER DEFAULT 0)')
    await db.execute('CREATE TABLE lines (id INTEGER PRIMARY KEY, ref TEXT, qty INTEGER)')

    const concurrent = 100
    const results = await Promise.all(
      Array.from({ length: concurrent }, (_, i) => db.executeTransaction(orderStatements(`order-${i}`))),
    )

    expect(results).toHaveLength(concurrent)
    expect(results.every(r => r.length === 3 && r.every(one => one.changes === 1))).toBe(true)

    const [{ c }] = await db.query<{ c: number }>('SELECT COUNT(*) AS c FROM orders WHERE total = 1')
    expect(c).toBe(concurrent)
    expect(counter.begins).toBeLessThanOrEqual(3)

    await sirannon.shutdown()
  })

  it('rolls back only the offending transaction, committing its neighbours whole', async () => {
    const sirannon = new Sirannon({ driver: betterSqlite3() })
    const db = await sirannon.open('main', join(dir, 'isot.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)')
    await db.execute('INSERT INTO t (v) VALUES (?)', ['taken'])

    const results = await Promise.allSettled([
      db.executeTransaction([
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['a1'] },
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['a2'] },
      ]),
      db.executeTransaction([
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['b1'] },
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['taken'] },
      ]),
      db.executeTransaction([
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['c1'] },
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['c2'] },
      ]),
    ])

    expect(results.map(r => r.status)).toEqual(['fulfilled', 'rejected', 'fulfilled'])

    const rows = await db.query<{ v: string }>('SELECT v FROM t ORDER BY v')
    expect(rows.map(r => r.v)).toEqual(['a1', 'a2', 'c1', 'c2', 'taken'])

    await sirannon.shutdown()
  })

  it('groups and isolates transactions through the writer worker', async () => {
    const sirannon = new Sirannon({ driver: betterSqlite3() })
    const db = await sirannon.open('main', join(dir, 'wt.db'), { synchronous: 'full', writerWorker: true })
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)')
    await db.execute('INSERT INTO t (v) VALUES (?)', ['taken'])

    const many = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        db.executeTransaction([
          { sql: 'INSERT INTO t (v) VALUES (?)', params: [`w-${i}-1`] },
          { sql: 'INSERT INTO t (v) VALUES (?)', params: [`w-${i}-2`] },
        ]),
      ),
    )
    expect(many.every(r => r.length === 2 && r.every(one => one.changes === 1))).toBe(true)

    const mixed = await Promise.allSettled([
      db.executeTransaction([
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['x1'] },
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['x2'] },
      ]),
      db.executeTransaction([
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['doomed'] },
        { sql: 'INSERT INTO t (v) VALUES (?)', params: ['taken'] },
      ]),
    ])
    expect(mixed.map(r => r.status)).toEqual(['fulfilled', 'rejected'])

    const doomed = await db.query('SELECT v FROM t WHERE v = ?', ['doomed'])
    expect(doomed).toHaveLength(0)

    const [{ c }] = await db.query<{ c: number }>('SELECT COUNT(*) AS c FROM t')
    expect(c).toBe(1 + 50 * 2 + 2)

    await sirannon.shutdown()
  })

  it('runs a transaction carrying DDL outside the group', async () => {
    const sirannon = new Sirannon({ driver: betterSqlite3() })
    const db = await sirannon.open('main', join(dir, 'ddlt.db'), { synchronous: 'full' })

    await db.executeTransaction([
      { sql: 'CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)' },
      { sql: 'INSERT INTO audit (note) VALUES (?)', params: ['opened'] },
    ])

    const rows = await db.query<{ note: string }>('SELECT note FROM audit')
    expect(rows.map(r => r.note)).toEqual(['opened'])

    await sirannon.shutdown()
  })
})
