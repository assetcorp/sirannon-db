import { copyFileSync, existsSync, statSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { countRaw, createWorkspace, type Workspace, WRITER_PLACEMENTS, walCommitCount } from './harness.js'

const ROWS = 300

type EntryPoint = [label: string, call: (db: Database, sql: string) => Promise<unknown>]

const ENTRY_POINTS: EntryPoint[] = [
  ['query', (db, sql) => db.query(sql)],
  ['queryOne', (db, sql) => db.queryOne(sql)],
  ['execute', (db, sql) => db.execute(sql)],
  ['executeBatch', (db, sql) => db.executeBatch(sql, [[]])],
  ['executeTransaction', (db, sql) => db.executeTransaction([{ sql: 'DELETE FROM notes WHERE id < 0' }, { sql }])],
  ['transaction execute', (db, sql) => db.transaction(tx => tx.execute(sql))],
  ['transaction query', (db, sql) => db.transaction(tx => tx.query(sql))],
]

const CHECKPOINT_SPELLINGS = [
  'PRAGMA wal_checkpoint',
  'PRAGMA wal_checkpoint(TRUNCATE)',
  'pragma WAL_CHECKPOINT(passive)',
  'PRAGMA main.wal_checkpoint(FULL)',
  'PRAGMA "wal_checkpoint"(RESTART)',
  "PRAGMA 'wal_checkpoint'",
  'PRAGMA [wal_checkpoint](TRUNCATE)',
  'PRAGMA main.`wal_checkpoint`',
  '/* first */ PRAGMA wal_checkpoint(TRUNCATE)',
  'SELECT 1; PRAGMA wal_checkpoint(TRUNCATE)',
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('checkpoint')
})

afterEach(async () => {
  await workspace.dispose()
})

function logBytes(path: string): number {
  const walPath = `${path}-wal`
  return existsSync(walPath) ? statSync(walPath).size : 0
}

function rowsInMainFileAlone(path: string): number {
  const copy = `${path}.main-file-only`
  copyFileSync(path, copy)
  const raw = new BetterSqlite3(copy, { readonly: true })
  try {
    const table = raw.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'notes'").get()
    if (table === undefined) return 0
    const row = raw.prepare('SELECT count(*) AS n FROM notes').get() as { n: number }
    return row.n
  } finally {
    raw.close()
  }
}

describe.each(WRITER_PLACEMENTS)('checkpoint with $label', ({ writerWorker }) => {
  async function openNotes(name = 'notes.db'): Promise<{ db: Database; path: string }> {
    const path = workspace.path(name)
    const db = await workspace.registry().open(name, path, { writerWorker })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.executeBatch(
      'INSERT INTO notes (id, body) VALUES (?, ?)',
      Array.from({ length: ROWS }, (_, i) => [i + 1, `note ${i + 1}`]),
    )
    return { db, path }
  }

  it('copies every committed write into the database file and empties the log', async () => {
    const { db, path } = await openNotes()
    expect(walCommitCount(path)).toBeGreaterThan(0)
    expect(rowsInMainFileAlone(path)).not.toBe(ROWS)

    const result = await db.checkpoint()

    expect(result).toEqual({ busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true })
    expect(logBytes(path)).toBe(0)
    expect(rowsInMainFileAlone(path)).toBe(ROWS)
    expect(countRaw(path, 'notes')).toBe(ROWS)
  })

  it('reports busy while a reader holds the log, keeps the frames in the log, and empties it once the reader ends', async () => {
    const { db, path } = await openNotes()
    await db.checkpoint()
    const reader = new BetterSqlite3(path, { readonly: true })
    try {
      reader.prepare('BEGIN').run()
      expect(reader.prepare('SELECT count(*) AS n FROM notes').get()).toEqual({ n: ROWS })
      await db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [ROWS + 1, 'after the reader'])
      const framesBefore = walCommitCount(path)

      const held = await db.checkpoint()

      expect(held.busy).toBe(true)
      expect(held.emptied).toBe(false)
      expect(held.framesInLog).toBeGreaterThan(0)
      expect(walCommitCount(path)).toBe(framesBefore)
      expect(reader.prepare('SELECT count(*) AS n FROM notes').get()).toEqual({ n: ROWS })
      reader.prepare('COMMIT').run()
    } finally {
      reader.close()
    }

    const released = await db.checkpoint()

    expect(released).toEqual({ busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true })
    expect(logBytes(path)).toBe(0)
    expect(rowsInMainFileAlone(path)).toBe(ROWS + 1)
  })

  it('refuses PRAGMA wal_checkpoint in every spelling through every entry point, points to checkpoint, and leaves the log as it was', async () => {
    const { db, path } = await openNotes()
    const frames = walCommitCount(path)
    const bytes = logBytes(path)

    for (const sql of CHECKPOINT_SPELLINGS) {
      for (const [label, call] of ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({
          code: 'FORBIDDEN_SQL',
          message: expect.stringContaining('checkpoint()'),
        })
      }
    }

    expect(walCommitCount(path)).toBe(frames)
    expect(logBytes(path)).toBe(bytes)
    expect(rowsInMainFileAlone(path)).not.toBe(ROWS)
    expect(await db.query('PRAGMA wal_autocheckpoint')).toEqual([{ wal_autocheckpoint: 1000 }])
    expect(await db.query("SELECT 'wal_checkpoint' AS word")).toEqual([{ word: 'wal_checkpoint' }])
  })

  it('fails inside a transaction callback without touching the log, and checkpoints once the transaction ends', async () => {
    const { db, path } = await openNotes()
    const frames = walCommitCount(path)

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [ROWS + 1, 'inside'])
        await db.checkpoint()
      }),
    ).rejects.toMatchObject({ code: 'CHECKPOINT_IN_TRANSACTION' })

    expect(walCommitCount(path)).toBe(frames)
    expect(countRaw(path, 'notes')).toBe(ROWS)
    expect((await db.checkpoint()).emptied).toBe(true)
    expect(rowsInMainFileAlone(path)).toBe(ROWS)
  })
})

describe('checkpoint on databases that cannot take one', () => {
  it('fails with READ_ONLY on a read-only database and leaves its log in place', async () => {
    const path = workspace.path('shared.db')
    const writer = await workspace.registry().open('writer', path)
    await writer.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await writer.execute("INSERT INTO notes (id, body) VALUES (1, 'kept in the log')")
    const frames = walCommitCount(path)
    const reader = await workspace.registry().open('reader', path, { readOnly: true })

    await expect(reader.checkpoint()).rejects.toMatchObject({ code: 'READ_ONLY' })

    expect(walCommitCount(path)).toBe(frames)
    expect(countRaw(path, 'notes')).toBe(1)
  })

  it('reports an empty log for a database without write-ahead logging', async () => {
    const path = workspace.path('rollback.db')
    const db = await workspace.registry().open('rollback', path, { walMode: false })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'in the file')")

    expect(await db.checkpoint()).toEqual({ busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true })
    expect(existsSync(`${path}-wal`)).toBe(false)
    expect(rowsInMainFileAlone(path)).toBe(1)
  })

  it('fails with DATABASE_CLOSED after the database closes', async () => {
    const path = workspace.path('closed.db')
    const sirannon = workspace.registry()
    const db = await sirannon.open('closed', path)
    await sirannon.close('closed')

    await expect(db.checkpoint()).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
  })
})
