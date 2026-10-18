import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { singleConnectionDriver } from '../../helpers/fault-drivers/single-connection-driver.js'

const HANG_DEADLINE_MS = 5_000

let dir: string
let sirannon: Sirannon

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-untracked-callers-'))
  sirannon = new Sirannon({ driver: singleConnectionDriver() })
})

afterEach(async () => {
  await sirannon.shutdown().catch(() => {})
  rmSync(dir, { recursive: true, force: true })
})

async function openNotes(name: string, watch: boolean): Promise<Database> {
  const db = await sirannon.open(name, join(dir, `${name}.db`))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  if (watch) await db.watch('notes')
  return db
}

function bodiesIn(name: string): string[] {
  const raw = new BetterSqlite3(join(dir, `${name}.db`), { readonly: true, fileMustExist: true })
  try {
    return raw
      .prepare('SELECT body FROM notes ORDER BY id')
      .all()
      .map(row => (row as { body: string }).body)
  } finally {
    raw.close()
  }
}

async function outcomeWithin(call: () => Promise<unknown>): Promise<string> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const hung = new Promise<string>(resolve => {
    timer = setTimeout(() => resolve('still waiting'), HANG_DEADLINE_MS)
  })
  const settled = call().then(
    () => 'fulfilled',
    (err: unknown) => (err as { code?: string }).code ?? String(err),
  )
  try {
    return await Promise.race([settled, hung])
  } finally {
    clearTimeout(timer)
  }
}

const TAGS_MIGRATION = {
  version: 1,
  name: 'create_tags',
  up: 'CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT NOT NULL)',
  down: 'DROP TABLE tags',
}

function callsThroughTheDatabase(db: Database, name: string): Record<string, () => Promise<unknown>> {
  return {
    execute: () => db.execute("INSERT INTO notes (body) VALUES ('nested execute')"),
    executeBatch: () => db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['nested batch']]),
    executeTransaction: () => db.executeTransaction([{ sql: "INSERT INTO notes (body) VALUES ('nested statements')" }]),
    transaction: () => db.transaction(tx => tx.execute("INSERT INTO notes (body) VALUES ('nested callback')")),
    bulkLoad: () => db.bulkLoad('INSERT INTO notes (body) VALUES (?)', [['nested load']]),
    query: () => db.query('SELECT count(*) AS n FROM notes'),
    queryOne: () => db.queryOne('SELECT count(*) AS n FROM notes'),
    migrate: () => db.migrate([TAGS_MIGRATION]),
    rollback: () => db.rollback([TAGS_MIGRATION], 0),
    appliedMigrations: () => db.appliedMigrations(),
    watch: () => db.watch('notes'),
    unwatch: () => db.unwatch('notes'),
    live: () => db.live('SELECT id, body FROM notes'),
    loadExtension: () => db.loadExtension(join(dir, 'missing-extension')),
    backup: () => db.backup(join(dir, `${name}-copy.db`)),
    captureBackupChanges: () => db.captureBackupChanges(),
  }
}

function tablesIn(name: string): string[] {
  const raw = new BetterSqlite3(join(dir, `${name}.db`), { readonly: true, fileMustExist: true })
  try {
    return raw
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sirannon%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
      )
      .all()
      .map(row => (row as { name: string }).name)
  } finally {
    raw.close()
  }
}

describe('a transaction callback on a driver without async context', () => {
  for (const watch of [false, true]) {
    it(`refuses every call through the database at once and commits the writes through the handle, ${watch ? 'watched' : 'unwatched'}`, async () => {
      const name = `nested-${watch}`
      const db = await openNotes(name, watch)
      const calls = callsThroughTheDatabase(db, name)

      const outcomes = await db.transaction(async tx => {
        await tx.execute("INSERT INTO notes (body) VALUES ('through the handle')")
        const seen: Record<string, string> = {}
        for (const [label, call] of Object.entries(calls)) seen[label] = await outcomeWithin(call)
        await tx.execute("INSERT INTO notes (body) VALUES ('after the refusals')")
        return seen
      })

      expect(outcomes).toEqual(Object.fromEntries(Object.keys(calls).map(label => [label, 'WRITER_HELD_BY_CALLBACK'])))
      expect(bodiesIn(name)).toEqual(['through the handle', 'after the refusals'])
      expect(tablesIn(name)).toEqual(['notes'])
      await db.execute("INSERT INTO notes (body) VALUES ('once the callback returned')")
      expect(bodiesIn(name)).toEqual(['through the handle', 'after the refusals', 'once the callback returned'])
      expect(await outcomeWithin(() => db.migrate([TAGS_MIGRATION]))).toBe('fulfilled')
      expect(tablesIn(name)).toEqual(['notes', 'tags'])
    })
  }

  it('refuses a caller outside the callback while it holds the writer, and serves every caller once it throws', async () => {
    const db = await openNotes('outside', true)
    let release: () => void = () => {}
    const held = new Promise<void>(resolve => {
      release = resolve
    })
    let entered: () => void = () => {}
    const inside = new Promise<void>(resolve => {
      entered = resolve
    })

    const abandoned = db.transaction(async tx => {
      await tx.execute("INSERT INTO notes (body) VALUES ('rolled back')")
      entered()
      await held
      throw new Error('the callback gives up')
    })
    const queuedBefore = db.execute("INSERT INTO notes (body) VALUES ('queued before the callback held the writer')")
    await inside

    const outside = await outcomeWithin(() => db.execute("INSERT INTO notes (body) VALUES ('from outside')"))
    release()

    await expect(abandoned).rejects.toThrow('the callback gives up')
    await queuedBefore
    expect(outside).toBe('WRITER_HELD_BY_CALLBACK')
    expect(await outcomeWithin(() => db.execute("INSERT INTO notes (body) VALUES ('after')"))).toBe('fulfilled')
    expect(bodiesIn('outside')).toEqual(['queued before the callback held the writer', 'after'])
  })
})
