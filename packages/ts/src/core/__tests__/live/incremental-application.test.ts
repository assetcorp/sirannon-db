import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import type { OpenOptions, SQLiteConnection, SQLiteDriver } from '../../driver/types.js'
import type { LiveQuery } from '../../live/types.js'
import { Sirannon } from '../../sirannon.js'
import { readyRows, waitForRows } from './_helpers.js'

const READ_MARKER = '_sirannon_k0'

interface ReadCounter {
  driver: SQLiteDriver
  reads(): number
}

function countingDriver(inner: SQLiteDriver): ReadCounter {
  let reads = 0
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
          if (!sql.includes(READ_MARKER)) return stmt
          return {
            ...stmt,
            all: (...params: unknown[]) => {
              reads++
              return stmt.all(...params)
            },
            get: (...params: unknown[]) => stmt.get(...params),
            run: (...params: unknown[]) => stmt.run(...params),
          }
        },
      }
      return wrapped
    },
  }
  return { driver, reads: () => reads }
}

let dir: string | undefined
let sirannon: Sirannon | undefined
let query: LiveQuery<Record<string, unknown>> | undefined

afterEach(async () => {
  await query?.close()
  query = undefined
  await sirannon?.shutdown().catch(() => {})
  sirannon = undefined
  if (dir !== undefined) rmSync(dir, { recursive: true, force: true })
  dir = undefined
})

async function openDatabase(): Promise<{ db: Database; counter: ReadCounter }> {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-live-count-'))
  const counter = countingDriver(betterSqlite3())
  sirannon = new Sirannon({ driver: counter.driver })
  const db = await sirannon.open('shop', join(dir, 'shop.db'), { cdcPollInterval: 5 })
  await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, bucket TEXT NOT NULL)')
  return { db, counter }
}

describe('live query change application', () => {
  it('reads once and applies every later change to the rows it holds', async () => {
    const { db, counter } = await openDatabase()
    const seed = Array.from({ length: 30 }, (_, index) => ({
      sql: 'INSERT INTO items VALUES (?, ?, ?)',
      params: [index + 1, `row-${index}`, 'a'],
    }))
    await db.executeTransaction(seed)

    const live = await db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'])
    query = live as LiveQuery<Record<string, unknown>>
    expect(counter.reads()).toBe(1)

    for (let index = 0; index < 10; index++) {
      await db.execute('INSERT INTO items VALUES (?, ?, ?)', [100 + index, `late-${index}`, 'a'])
      await waitForRows(live, rows => rows.length === 31 + index)
    }

    expect(live.getState().status).toBe('ready')
    expect(counter.reads()).toBe(1)
  })

  it('leaves the read alone for changes to rows outside the result', async () => {
    const { db, counter } = await openDatabase()
    await db.execute("INSERT INTO items VALUES (1, 'kept', 'a')")

    const live = await db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'])
    query = live as LiveQuery<Record<string, unknown>>

    for (let index = 0; index < 20; index++) {
      await db.execute('INSERT INTO items VALUES (?, ?, ?)', [200 + index, `other-${index}`, 'b'])
    }
    await new Promise(resolve => setTimeout(resolve, 200))

    expect(readyRows(live)).toEqual([{ id: 1 }])
    expect(counter.reads()).toBe(1)
  })

  it('reads again only when a transaction outnumbers the rows it holds', async () => {
    const { db, counter } = await openDatabase()
    await db.execute("INSERT INTO items VALUES (1, 'only', 'a')")

    const live = await db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'], {
      rereadJitterMs: 0,
    })
    query = live as LiveQuery<Record<string, unknown>>
    expect(counter.reads()).toBe(1)

    await db.executeTransaction(
      Array.from({ length: 8 }, (_, index) => ({
        sql: 'INSERT INTO items VALUES (?, ?, ?)',
        params: [index + 10, `bulk-${index}`, 'a'],
      })),
    )
    await waitForRows(live, rows => rows.length === 9)
    expect(counter.reads()).toBe(2)

    await db.execute("INSERT INTO items VALUES (99, 'single', 'a')")
    await waitForRows(live, rows => rows.length === 10)
    expect(counter.reads()).toBe(2)
  })

  it('re-reads instead of holding a transaction larger than its buffer', async () => {
    const { db, counter } = await openDatabase()
    const seed = Array.from({ length: 40 }, (_, index) => ({
      sql: 'INSERT INTO items VALUES (?, ?, ?)',
      params: [index + 1, `row-${index}`, 'a'],
    }))
    await db.executeTransaction(seed)

    const live = await db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'], {
      rereadJitterMs: 0,
      maxTransactionChanges: 3,
    })
    query = live as LiveQuery<Record<string, unknown>>
    expect(counter.reads()).toBe(1)

    await db.executeTransaction(
      Array.from({ length: 10 }, (_, index) => ({
        sql: 'INSERT INTO items VALUES (?, ?, ?)',
        params: [index + 100, `bulk-${index}`, 'a'],
      })),
    )

    await waitForRows(live, rows => rows.length === 50)
    expect(counter.reads()).toBe(2)
  })
})
