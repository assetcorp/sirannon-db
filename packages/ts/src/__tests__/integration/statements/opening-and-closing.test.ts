import { writeFileSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { SynchronousLevel } from '../../../core/driver/types.js'
import type { ConnectionMetrics } from '../../../core/types.js'
import {
  columnRaw,
  countRaw,
  createWorkspace,
  DRIVER_PLACEMENTS,
  DRIVERS,
  journalFormat,
  readRaw,
  type Workspace,
} from './harness.js'

const SQLITE_SYNCHRONOUS_NUMBERS: Record<SynchronousLevel, number> = { off: 0, normal: 1, full: 2, extra: 3 }
const CONCURRENT_CALLS = 100
const REOPEN_CYCLES = 8

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('opening-and-closing')
})

afterEach(async () => {
  await workspace.dispose()
})

function seedFile(path: string, names: readonly string[]): void {
  const raw = new BetterSqlite3(path)
  try {
    raw.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    const insert = raw.prepare('INSERT INTO items (name) VALUES (?)')
    for (const name of names) insert.run(name)
  } finally {
    raw.close()
  }
}

const EVERY_CALL: [label: string, call: (db: Database) => Promise<unknown>][] = [
  ['query', db => db.query('SELECT 1')],
  ['queryOne', db => db.queryOne('SELECT 1')],
  ['execute', db => db.execute("INSERT INTO items (name) VALUES ('x')")],
  ['executeBatch', db => db.executeBatch('INSERT INTO items (name) VALUES (?)', [['x']])],
  ['executeTransaction', db => db.executeTransaction([{ sql: "INSERT INTO items (name) VALUES ('x')" }])],
  ['empty executeTransaction', db => db.executeTransaction([])],
  ['transaction', db => db.transaction(async () => 'never')],
  ['bulkLoad', db => db.bulkLoad('INSERT INTO items (name) VALUES (?)', [['x']])],
  ['watch', db => db.watch('items')],
  ['live', db => db.live('SELECT name FROM items')],
]

describe.each(DRIVER_PLACEMENTS)('opening and closing with $label', ({ driver, writerWorker }) => {
  it('refuses every write on a read-only database with READ_ONLY, and reads it as the file holds it', async () => {
    const path = workspace.path('catalogue.db')
    seedFile(path, ['kettle', 'teapot'])
    const before = readRaw(path, 'SELECT * FROM items ORDER BY id')
    const db = await workspace.registry({ driver: driver() }).open('catalogue', path, { readOnly: true, writerWorker })

    expect(db.readOnly).toBe(true)
    for (const [label, call] of EVERY_CALL.filter(([name]) => name !== 'query' && name !== 'queryOne')) {
      await expect(call(db), label).rejects.toMatchObject({ code: 'READ_ONLY' })
    }
    await expect(db.query('DELETE FROM items')).rejects.toMatchObject({ code: 'QUERY_ERROR' })
    expect(await db.query('SELECT id, name FROM items ORDER BY id')).toEqual([
      { id: 1, name: 'kettle' },
      { id: 2, name: 'teapot' },
    ])
    expect(readRaw(path, 'SELECT * FROM items ORDER BY id')).toEqual(before)
  })

  it('refuses a write sent through the read API of a writable database', async () => {
    const path = workspace.path('writable.db')
    seedFile(path, ['kettle'])
    const db = await workspace.registry({ driver: driver() }).open('writable', path, { writerWorker })

    for (const sql of ["INSERT INTO items (name) VALUES ('sneaked')", 'DELETE FROM items', 'DROP TABLE items']) {
      await expect(db.query(sql), sql).rejects.toMatchObject({ code: 'QUERY_ERROR', sql })
      await expect(db.queryOne(sql), sql).rejects.toMatchObject({ code: 'QUERY_ERROR', sql })
    }
    expect(columnRaw(path, 'SELECT name FROM items')).toEqual(['kettle'])
  })

  it('fails every call with DATABASE_CLOSED after close, and finishes closing before any close call resolves', async () => {
    const path = workspace.path('closing.db')
    seedFile(path, ['kettle'])
    const closes: string[] = []
    const closedConnections: ConnectionMetrics[] = []
    const sirannon = workspace.registry({
      driver: driver(),
      metrics: { onConnectionClose: metrics => closedConnections.push(metrics) },
    })
    for (const label of ['first', 'second', 'third']) {
      sirannon.onDatabaseClose(context => {
        closes.push(`${label}:${context.databaseId}`)
      })
    }
    const db = await sirannon.open('closing', path, { writerWorker })
    await db.execute("INSERT INTO items (name) VALUES ('teapot')")
    expect({ id: db.id, path: db.path, readOnly: db.readOnly, closed: db.closed }).toEqual({
      id: 'closing',
      path,
      readOnly: false,
      closed: false,
    })

    const firstClose = db.close()
    await db.close()
    expect(closes).toEqual(['first:closing', 'second:closing', 'third:closing'])
    await firstClose
    await db.close()

    expect(db.closed).toBe(true)
    expect(sirannon.has('closing')).toBe(false)
    expect(closes).toHaveLength(3)
    expect(closedConnections.map(metrics => metrics.databaseId)).toEqual(['closing'])
    for (const [label, call] of EVERY_CALL) {
      await expect(call(db), label).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    }
    expect(() => db.on('items')).toThrow(expect.objectContaining({ code: 'DATABASE_CLOSED' }))
    await expect(sirannon.close('closing')).rejects.toMatchObject({ code: 'DATABASE_NOT_FOUND' })
    expect(columnRaw(path, 'SELECT name FROM items ORDER BY id')).toEqual(['kettle', 'teapot'])
  })

  it('keeps every row through repeated closes and reopens under changing settings', async () => {
    const path = workspace.path('reopened.db')
    const sirannon = workspace.registry({ driver: driver() })
    const settings = [
      { writerWorker },
      { writerWorker, readPoolSize: 1 },
      { writerWorker: !writerWorker, synchronous: 'full' as const },
      { writerWorker, readPoolSize: 6, synchronous: 'off' as const },
    ]

    const first = await sirannon.open('reopened', path, { writerWorker })
    await first.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    await first.close()
    for (let cycle = 0; cycle < REOPEN_CYCLES; cycle++) {
      const db = await sirannon.open('reopened', path, settings[cycle % settings.length])
      const pending = Array.from({ length: 10 }, (_, index) =>
        db.execute('INSERT INTO items (name) VALUES (?)', [`cycle-${cycle}-${index}`]),
      )
      if (cycle % 2 === 0) {
        await Promise.all(pending)
      } else {
        const closing = db.close()
        await expect(Promise.all(pending)).resolves.toHaveLength(10)
        await closing
      }
      if (!db.closed) await db.close()
      expect(countRaw(path, 'items')).toBe((cycle + 1) * 10)
    }

    const readOnly = await sirannon.open('reopened', path, { readOnly: true, writerWorker })
    expect(await readOnly.queryOne('SELECT count(*) AS n FROM items')).toEqual({ n: REOPEN_CYCLES * 10 })
  })
})

describe.each(DRIVERS)('settings with $label', ({ driver }) => {
  it('opens as many readers as readPoolSize asks for, never fewer than one, and serves concurrent reads from them', async () => {
    const opened: ConnectionMetrics[] = []
    const sirannon = workspace.registry({
      driver: driver(),
      metrics: { onConnectionOpen: metrics => opened.push(metrics) },
    })
    const path = workspace.path('readers.db')
    seedFile(path, ['kettle', 'teapot', 'strainer'])

    for (const readPoolSize of [0, -3, 1, 6]) {
      const db = await sirannon.open(`readers-${readPoolSize}`, path, { readPoolSize })
      const expected = Math.max(readPoolSize, 1)
      expect(db.readerCount).toBe(expected)
      expect(opened.at(-1)).toMatchObject({ databaseId: `readers-${readPoolSize}`, readerCount: expected })
      const counts = await Promise.all(
        Array.from({ length: CONCURRENT_CALLS }, () => db.queryOne<{ n: number }>('SELECT count(*) AS n FROM items')),
      )
      expect(counts).toEqual(Array(CONCURRENT_CALLS).fill({ n: 3 }))
      await db.close()
    }
  })

  it('writes the file in WAL mode by default and in rollback-journal mode when walMode is false', async () => {
    const sirannon = workspace.registry({ driver: driver() })
    const walPath = workspace.path('wal.db')
    const rollbackPath = workspace.path('rollback.db')

    const wal = await sirannon.open('wal', walPath)
    const rollback = await sirannon.open('rollback', rollbackPath, { walMode: false })
    for (const db of [wal, rollback]) {
      await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
      const writes = Array.from({ length: CONCURRENT_CALLS }, (_, index) =>
        db.execute('INSERT INTO items (name) VALUES (?)', [`item-${index}`]),
      )
      const reads = Array.from({ length: CONCURRENT_CALLS }, () => db.query('SELECT name FROM items'))
      await Promise.all([...writes, ...reads])
    }

    expect(journalFormat(walPath)).toBe('wal')
    expect(journalFormat(rollbackPath)).toBe('rollback')
    expect(readRaw(rollbackPath, 'PRAGMA journal_mode')).toEqual([{ journal_mode: 'delete' }])
    expect(countRaw(walPath, 'items')).toBe(CONCURRENT_CALLS)
    expect(countRaw(rollbackPath, 'items')).toBe(CONCURRENT_CALLS)
  })

  it('applies the synchronous level asked for on the writer and refuses an unknown level', async () => {
    const sirannon = workspace.registry({ driver: driver() })

    for (const [level, number] of Object.entries(SQLITE_SYNCHRONOUS_NUMBERS)) {
      const db = await sirannon.open(`sync-${level}`, workspace.path(`sync-${level}.db`), {
        synchronous: level as SynchronousLevel,
      })
      expect(await db.transaction(tx => tx.query('PRAGMA synchronous'))).toEqual([{ synchronous: number }])
      await db.close()
    }
    for (const level of ['fast', 'NORMAL', '', 'normal; DROP TABLE items']) {
      await expect(
        sirannon.open('sync-bad', workspace.path('sync-bad.db'), { synchronous: level as SynchronousLevel }),
        level,
      ).rejects.toMatchObject({ code: 'INVALID_SYNCHRONOUS' })
      expect(sirannon.has('sync-bad')).toBe(false)
    }
  })

  it('refuses an identifier in use, a path in a missing directory, and a file that is not a database', async () => {
    const sirannon = workspace.registry({ driver: driver() })
    const path = workspace.path('taken.db')
    const garbage = workspace.path('garbage.db')
    writeFileSync(garbage, 'these bytes are a shopping list, not a SQLite database file')
    await sirannon.open('taken', path)

    await expect(sirannon.open('taken', workspace.path('other.db'))).rejects.toMatchObject({
      code: 'DATABASE_ALREADY_EXISTS',
    })
    await expect(sirannon.open('missing', workspace.path('no/such/directory/file.db'))).rejects.toMatchObject({
      code: 'DATABASE_OPEN_FAILED',
    })
    await expect(sirannon.open('garbage', garbage)).rejects.toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect([...sirannon.databases().keys()]).toEqual(['taken'])
  })
})
