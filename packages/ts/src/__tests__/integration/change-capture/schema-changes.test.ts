import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import {
  collect,
  createWorkspace,
  PLACEMENTS,
  pause,
  QUIET_PERIOD_MS,
  readRaw,
  type Workspace,
  waitFor,
  waitForCount,
  writeRaw,
} from './harness.js'

const ROLLBACK = new Error('the caller abandons the transaction')

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('schema')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openNotes(name: string): Promise<Database> {
  const db = await workspace.open(name)
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  await db.watch('notes')
  return db
}

function columnsOf(path: string, table: string): string[] {
  return readRaw<{ name: string }>(path, `SELECT name FROM pragma_table_info('${table}')`).map(column => column.name)
}

describe('a column added to a watched table', () => {
  it('appears in later changes whether the ALTER runs alone, omits the COLUMN keyword, or shares a transaction with writes', async () => {
    const db = await openNotes('added')
    const seen = collect(db.on('notes'))

    await db.execute("INSERT INTO notes VALUES (1, 'plain')")
    await db.execute('ALTER TABLE notes ADD COLUMN mood TEXT')
    await db.execute("INSERT INTO notes VALUES (2, 'with mood', 'calm')")
    await db.execute('/* added by hand */ alter table notes add priority INTEGER')
    await db.execute("INSERT INTO notes VALUES (3, 'with priority', 'tense', 9)")
    await db.transaction(async tx => {
      await tx.execute("ALTER TABLE notes ADD COLUMN tag TEXT DEFAULT 'none'")
      await tx.execute("INSERT INTO notes VALUES (4, 'same transaction', 'calm', 1, 'urgent')")
    })
    await db.executeTransaction([
      { sql: 'ALTER TABLE notes ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0' },
      { sql: "INSERT INTO notes VALUES (5, 'listed statements', NULL, NULL, 'none', 1)" },
    ])
    await waitForCount(seen, 5)

    expect(seen.events.map(event => event.row)).toEqual([
      { id: 1, body: 'plain' },
      { id: 2, body: 'with mood', mood: 'calm' },
      { id: 3, body: 'with priority', mood: 'tense', priority: 9 },
      { id: 4, body: 'same transaction', mood: 'calm', priority: 1, tag: 'urgent' },
      { id: 5, body: 'listed statements', mood: null, priority: null, tag: 'none', pinned: 1 },
    ])
  })

  it('appears in later changes when another connection adds it while the database is closed', async () => {
    const registry = workspace.registry()
    const path = workspace.path('closed')
    const db = await registry.open('closed', path, { cdcPollInterval: 5 })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.watch('notes')
    await registry.close('closed')

    writeRaw(path, 'ALTER TABLE notes ADD COLUMN mood TEXT')

    const reopened = await registry.open('closed', path, { cdcPollInterval: 5 })
    await reopened.watch('notes')
    const seen = collect(reopened.on('notes'))
    await reopened.execute("INSERT INTO notes VALUES (1, 'after reopening', 'curious')")
    await waitForCount(seen, 1)

    expect(seen.events.map(event => event.row)).toEqual([{ id: 1, body: 'after reopening', mood: 'curious' }])
  })
})

function triggerSqlOf(path: string): string {
  return readRaw<{ sql: string }>(path, "SELECT sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")
    .map(trigger => trigger.sql)
    .join('\n')
}

describe('a column dropped from a watched table', () => {
  it.each(
    PLACEMENTS,
  )('leaves later changes without it whether the drop runs alone, in a transaction, in a statement list, in a batch, in a bulk load, or in a migration, with $label', async placement => {
    const path = workspace.path('dropped-column')
    const db = await workspace.open('dropped-column', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute(
      'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT, g TEXT, h TEXT)',
    )
    await db.watch('notes')
    const seen = collect(db.on('notes'))

    await db.execute("INSERT INTO notes VALUES (1, 'all columns', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')")
    await db.execute('ALTER TABLE notes DROP COLUMN a')
    await db.execute("UPDATE notes SET body = 'without a' WHERE id = 1")
    await db.execute('/* by hand */ alter table NOTES drop b')
    await db.transaction(async tx => {
      await tx.execute("INSERT INTO notes VALUES (2, 'before the drop', 'c', 'd', 'e', 'f', 'g', 'h')")
      await tx.execute('ALTER TABLE "notes" DROP COLUMN "c"')
      await tx.execute("INSERT INTO notes VALUES (3, 'after the drop', 'd', 'e', 'f', 'g', 'h')")
    })
    await db.executeTransaction([
      { sql: 'ALTER TABLE main.notes DROP COLUMN d' },
      { sql: "INSERT INTO notes VALUES (4, 'listed statements', 'e', 'f', 'g', 'h')" },
    ])
    await db.executeBatch('ALTER TABLE notes DROP COLUMN f', [[]])
    await db.execute("INSERT INTO notes VALUES (5, 'after a batch', 'e', 'g', 'h')")
    await db.bulkLoad('ALTER TABLE notes DROP COLUMN g', [[]])
    await db.execute("INSERT INTO notes VALUES (6, 'after a bulk load', 'e', 'h')")
    await db.transaction(async tx => {
      await tx.executeBatch('ALTER TABLE notes DROP COLUMN h', [[]])
      await tx.execute("INSERT INTO notes VALUES (7, 'after a batch in a transaction', 'e')")
    })
    const scripted = {
      version: 1,
      name: 'drop_e_and_add_it_back',
      up: "ALTER TABLE notes DROP COLUMN e; INSERT INTO notes VALUES (8, 'inside the script'); ALTER TABLE notes ADD COLUMN e TEXT",
    }
    await db.migrate([scripted])
    await db.execute("INSERT INTO notes VALUES (9, 'added back', 'again')")
    await db.migrate([
      scripted,
      {
        version: 2,
        name: 'drop_e_from_a_function',
        up: async tx => {
          await tx.execute('ALTER TABLE notes DROP COLUMN e')
          await tx.execute('DELETE FROM notes WHERE id = 8')
        },
      },
    ])
    await db.execute("INSERT INTO notes VALUES (10, 'last')")
    await waitForCount(seen, 12)

    expect(columnsOf(path, 'notes')).toEqual(['id', 'body'])
    expect(triggerSqlOf(path)).not.toMatch(/"(?:a|b|c|d|e|f|g|h)"/)
    expect(seen.events.map(event => [event.type, event.row, event.oldRow])).toEqual([
      [
        'insert',
        { id: 1, body: 'all columns', a: 'a', b: 'b', c: 'c', d: 'd', e: 'e', f: 'f', g: 'g', h: 'h' },
        undefined,
      ],
      [
        'update',
        { id: 1, body: 'without a', b: 'b', c: 'c', d: 'd', e: 'e', f: 'f', g: 'g', h: 'h' },
        { id: 1, body: 'all columns', b: 'b', c: 'c', d: 'd', e: 'e', f: 'f', g: 'g', h: 'h' },
      ],
      ['insert', { id: 2, body: 'before the drop', c: 'c', d: 'd', e: 'e', f: 'f', g: 'g', h: 'h' }, undefined],
      ['insert', { id: 3, body: 'after the drop', d: 'd', e: 'e', f: 'f', g: 'g', h: 'h' }, undefined],
      ['insert', { id: 4, body: 'listed statements', e: 'e', f: 'f', g: 'g', h: 'h' }, undefined],
      ['insert', { id: 5, body: 'after a batch', e: 'e', g: 'g', h: 'h' }, undefined],
      ['insert', { id: 6, body: 'after a bulk load', e: 'e', h: 'h' }, undefined],
      ['insert', { id: 7, body: 'after a batch in a transaction', e: 'e' }, undefined],
      ['insert', { id: 8, body: 'inside the script' }, undefined],
      ['insert', { id: 9, body: 'added back', e: 'again' }, undefined],
      ['delete', {}, { id: 8, body: 'inside the script' }],
      ['insert', { id: 10, body: 'last' }, undefined],
    ])
  })

  it('keeps capturing with the old columns when the drop fails, and with the new ones after a reopen', async () => {
    const path = workspace.path('failed-drop')
    const registry = workspace.registry()
    const db = await registry.open('failed-drop', path, { cdcPollInterval: 5 })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, code TEXT UNIQUE, mood TEXT)')
    await db.watch('notes')
    const seen = collect(db.on('notes'))

    const failures = [
      'ALTER TABLE notes DROP COLUMN code',
      'ALTER TABLE notes DROP COLUMN id',
      'ALTER TABLE notes DROP COLUMN missing',
    ]
    for (const sql of failures) {
      await expect(db.execute(sql), sql).rejects.toMatchObject({ code: 'QUERY_ERROR' })
    }
    await expect(
      db.transaction(async tx => {
        await tx.execute('ALTER TABLE notes DROP COLUMN mood')
        throw ROLLBACK
      }),
    ).rejects.toBe(ROLLBACK)
    await expect(
      db.migrate([
        { version: 1, name: 'drop_then_fail', up: 'ALTER TABLE notes DROP COLUMN mood; SELECT * FROM missing_table' },
      ]),
    ).rejects.toMatchObject({ code: 'MIGRATION_ERROR' })
    await db.execute("INSERT INTO notes VALUES (1, 'old columns', 'c1', 'calm')")
    await waitForCount(seen, 1)
    await db.execute('ALTER TABLE notes DROP COLUMN mood')
    await registry.close('failed-drop')

    const reopened = await workspace.registry().open('failed-drop', path, { cdcPollInterval: 5 })
    await reopened.watch('notes')
    const after = collect(reopened.on('notes'))
    await reopened.execute("INSERT INTO notes VALUES (2, 'after reopening', 'c2')")
    await waitFor(() => after.events.some(event => event.row.id === 2), 'the change written after reopening')

    expect(seen.events.map(event => event.row)).toEqual([{ id: 1, body: 'old columns', code: 'c1', mood: 'calm' }])
    expect(after.events.find(event => event.row.id === 2)?.row).toEqual({ id: 2, body: 'after reopening', code: 'c2' })
    expect(columnsOf(path, 'notes')).toEqual(['id', 'body', 'code'])
  })
})

describe('a schema change that rolls back', () => {
  it('leaves capture working with the columns the table kept', async () => {
    const db = await openNotes('rolled-back')
    const seen = collect(db.on('notes'))

    await expect(
      db.transaction(async tx => {
        await tx.execute('ALTER TABLE notes ADD COLUMN mood TEXT')
        throw ROLLBACK
      }),
    ).rejects.toBe(ROLLBACK)
    await expect(
      db.transaction(async tx => {
        await tx.execute('DROP TABLE notes')
        throw ROLLBACK
      }),
    ).rejects.toBe(ROLLBACK)
    await expect(
      db.executeTransaction([
        { sql: 'ALTER TABLE notes RENAME TO archived_notes' },
        { sql: 'INSERT INTO missing_table VALUES (1)' },
      ]),
    ).rejects.toThrow(/no such table/)
    await db.execute("INSERT INTO notes VALUES (1, 'still captured')")
    await waitForCount(seen, 1)

    expect(columnsOf(workspace.path('rolled-back'), 'notes')).toEqual(['id', 'body'])
    expect(seen.events.map(event => [event.table, event.row])).toEqual([['notes', { id: 1, body: 'still captured' }]])
  })
})

describe('a dropped watched table', () => {
  it('stops being captured, and its replacement is captured once it is watched', async () => {
    const db = await openNotes('dropped')
    await db.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
    await db.watch('tags')
    const notes = collect(db.on('notes'))
    const tags = collect(db.on('tags'))

    await db.execute("INSERT INTO notes VALUES (1, 'before the drop')")
    await db.execute('DROP TABLE notes')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT, starred INTEGER)')
    await db.execute("INSERT INTO notes VALUES (2, 'unwatched replacement', 0)")
    await db.transaction(async tx => {
      await tx.execute('DROP TABLE tags')
      await tx.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
      await tx.execute("INSERT INTO tags VALUES (1, 'inside the replacing transaction')")
    })
    await db.execute("INSERT INTO tags VALUES (2, 'unwatched replacement')")
    await expect(db.unwatch('notes')).resolves.toBeUndefined()
    await db.watch('notes')
    await db.watch('tags')
    await db.execute("INSERT INTO notes VALUES (3, 'watched again', 1)")
    await db.execute("INSERT INTO tags VALUES (3, 'watched again')")
    await waitForCount(notes, 2)
    await waitForCount(tags, 1)

    expect(notes.events.map(event => event.row)).toEqual([
      { id: 1, body: 'before the drop' },
      { id: 3, title: 'watched again', starred: 1 },
    ])
    expect(tags.events.map(event => event.row)).toEqual([{ id: 3, label: 'watched again' }])
  })
})

describe('a renamed column', () => {
  it('appears under its new name in later changes', async () => {
    const db = await openNotes('renamed-column')
    const seen = collect(db.on('notes'))

    await db.execute("INSERT INTO notes VALUES (1, 'old name')")
    await db.execute('ALTER TABLE notes RENAME COLUMN body TO content')
    await db.execute("INSERT INTO notes VALUES (2, 'new name')")
    await db.transaction(async tx => {
      await tx.execute('ALTER TABLE notes RENAME content TO text')
      await tx.execute("UPDATE notes SET text = 'renamed twice' WHERE id = 2")
    })
    await waitForCount(seen, 3)

    expect(seen.events.map(event => [event.type, event.row, event.oldRow])).toEqual([
      ['insert', { id: 1, body: 'old name' }, undefined],
      ['insert', { id: 2, content: 'new name' }, undefined],
      ['update', { id: 2, text: 'renamed twice' }, { id: 2, text: 'new name' }],
    ])
  })
})

describe('a renamed watched table', () => {
  it('stops being captured under its old name and is captured under its new name once that is watched', async () => {
    const db = await openNotes('renamed-table')
    const before = collect(db.on('notes'))
    const after = collect(db.on('journal'))

    await db.execute("INSERT INTO notes VALUES (1, 'before the rename')")
    await db.execute('ALTER TABLE notes RENAME TO journal')
    await db.execute("INSERT INTO journal VALUES (2, 'renamed but unwatched')")
    await db.watch('journal')
    await db.execute("INSERT INTO journal VALUES (3, 'watched under the new name')")
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute("INSERT INTO notes VALUES (4, 'a new table with the old name')")
    await waitForCount(before, 1)
    await waitForCount(after, 1)

    expect(before.events.map(event => [event.table, event.row])).toEqual([
      ['notes', { id: 1, body: 'before the rename' }],
    ])
    expect(after.events.map(event => [event.table, event.row])).toEqual([
      ['journal', { id: 3, body: 'watched under the new name' }],
    ])
  })
})

describe('schema changes racing writes', () => {
  it('capture every write once, and a write that commits after a column is added carries that column', async () => {
    const db = await openNotes('racing')
    const seen = collect(db.on('notes'))
    const writes = 60
    const added = ['c1', 'c2', 'c3', 'c4', 'c5']

    const pending: Promise<unknown>[] = []
    for (let id = 1; id <= writes; id++) {
      pending.push(db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note-${id}`]))
      if (id % 12 === 0) pending.push(db.execute(`ALTER TABLE notes ADD COLUMN ${added[id / 12 - 1]} TEXT DEFAULT 'd'`))
    }
    await Promise.all(pending)
    await db.execute("INSERT INTO notes (id, body) VALUES (1000, 'last')")
    await waitForCount(seen, writes + 1)
    await pause(QUIET_PERIOD_MS)

    expect(seen.events.map(event => event.row.id)).toHaveLength(writes + 1)
    expect(new Set(seen.events.map(event => event.row.id)).size).toBe(writes + 1)
    let widest = 0
    for (const event of seen.events) {
      const columns = Object.keys(event.row)
      expect(columns.slice(0, 2)).toEqual(['id', 'body'])
      expect(event.row.body).toBe(event.row.id === 1000 ? 'last' : `note-${event.row.id}`)
      expect(columns.slice(2)).toEqual(added.slice(0, columns.length - 2))
      for (const column of columns.slice(2)) expect(event.row[column]).toBe('d')
      expect(columns.length).toBeGreaterThanOrEqual(widest)
      widest = columns.length
    }
    expect(Object.keys(seen.events.at(-1)?.row ?? {})).toEqual(['id', 'body', ...added])
  })
})
