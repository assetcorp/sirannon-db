import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { collect, createWorkspace, PLACEMENTS, readRaw, type Workspace, waitFor } from './harness.js'

interface LoggedChange {
  operation: string
  row_id: string
  new_data: string | null
}

const COPY_KEEPING_COLUMNS = `
CREATE TABLE notes_new (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
INSERT INTO notes_new SELECT id, body, CAST(score AS INTEGER) FROM notes;
DROP TABLE notes;
ALTER TABLE notes_new RENAME TO notes;
INSERT INTO notes VALUES (100, 'written by the migration after the copy', 7);
`

const COPY_WITH_MIDDLE_COLUMN = `
CREATE TABLE notes_new (id INTEGER PRIMARY KEY, title TEXT, body TEXT NOT NULL, score TEXT);
INSERT INTO notes_new (id, body, score) SELECT id, body, score FROM notes;
DROP TABLE notes;
ALTER TABLE notes_new RENAME TO notes;
UPDATE notes SET title = 'set by the migration' WHERE id = 1;
`

const COPY_AFTER_RENAMING_FIRST = `
ALTER TABLE notes RENAME TO notes_old;
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
INSERT INTO notes SELECT id, body, CAST(score AS INTEGER) FROM notes_old;
DROP TABLE notes_old;
`

const COPY_AFTER_INDEXING_THE_NEW_TABLE = `
ALTER TABLE notes RENAME TO notes_old;
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
CREATE INDEX notes_body ON notes(body);
INSERT INTO notes SELECT id, body, CAST(score AS INTEGER) FROM notes_old;
DROP TABLE notes_old;
`

const COPY_INTO_TWO_NEW_TABLES = `
ALTER TABLE notes RENAME TO notes_old;
ALTER TABLE tags RENAME TO tags_old;
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
CREATE TABLE tags (id INTEGER PRIMARY KEY, note_id INTEGER NOT NULL, label TEXT NOT NULL);
INSERT INTO notes SELECT id, body, CAST(score AS INTEGER) FROM notes_old;
INSERT INTO tags SELECT id, note_id, label FROM tags_old;
DROP TABLE notes_old;
DROP TABLE tags_old;
`

const SQLITE_DOCUMENTED_REBUILD = `
CREATE TABLE new_notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
INSERT INTO new_notes SELECT id, body, CAST(score AS INTEGER) FROM notes;
DROP TABLE notes;
ALTER TABLE new_notes RENAME TO notes;
CREATE INDEX notes_body ON notes(body);
UPDATE notes SET body = 'written by the migration after the index' WHERE id = 2;
`

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('migration-copies')
})

afterEach(async () => {
  await workspace.dispose()
})

function changesOf(path: string): LoggedChange[] {
  return readRaw<LoggedChange>(
    path,
    "SELECT operation, row_id, new_data FROM _sirannon_changes WHERE table_name = 'notes' ORDER BY seq",
  )
}

function captureTriggersOf(path: string, table: string): string[] {
  return readRaw<{ name: string }>(
    path,
    "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? AND name LIKE '\\_sirannon\\_trg\\_%' ESCAPE '\\' ORDER BY name",
    [table],
  ).map(row => row.name)
}

async function seedNotes(db: Database): Promise<void> {
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score TEXT)')
  await db.watch('notes')
  await db.execute("INSERT INTO notes VALUES (1, 'first', '5'), (2, 'second', '6')")
}

describe.each(PLACEMENTS)('a migration that copies a watched table, $label', placement => {
  it('keeps capturing every write after the copy and leaves the copied rows out of the change log', async () => {
    const db = await workspace.open('same-columns', { writerWorker: placement.writerWorker }, placement.driver())
    await seedNotes(db)
    const seen = collect(db.on('notes'))

    await db.migrate([{ version: 1, name: 'score_as_integer', up: COPY_KEEPING_COLUMNS }])
    await db.execute("UPDATE notes SET body = 'after the migration' WHERE id = 2")
    await waitFor(() => seen.events.some(event => event.type === 'update'), 'the update written after the migration')

    const path = workspace.path('same-columns')
    expect(changesOf(path).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '100'],
      ['UPDATE', '2'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
    expect(seen.events.map(event => [event.type, event.row.id])).toEqual([
      ['insert', 1],
      ['insert', 2],
      ['insert', 100],
      ['update', 2],
    ])
  })
})

describe('a migration that copies a watched table into a new column order', () => {
  it('records the write that the migration makes after the copy with the new columns', async () => {
    const db = await workspace.open('middle-column')
    await seedNotes(db)

    await db.migrate([{ version: 1, name: 'title_before_body', up: COPY_WITH_MIDDLE_COLUMN }])

    const changes = changesOf(workspace.path('middle-column'))
    expect(changes.map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['UPDATE', '1'],
    ])
    expect(JSON.parse(changes[2]?.new_data ?? 'null')).toEqual({
      id: 1,
      title: 'set by the migration',
      body: 'first',
      score: '5',
    })
  })

  it('leaves the rows copied into a new table under the old name out of the change log when the migration renames first', async () => {
    const db = await workspace.open('renamed-first')
    await seedNotes(db)

    await db.migrate([{ version: 1, name: 'score_as_integer', up: COPY_AFTER_RENAMING_FIRST }])
    await db.execute("INSERT INTO notes VALUES (3, 'after the migration', 8)")

    const path = workspace.path('renamed-first')
    expect(changesOf(path).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '3'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
  })

  it('keeps capturing when a function migration copies the table', async () => {
    const db = await workspace.open('function-copy')
    await seedNotes(db)

    await db.migrate([
      {
        version: 1,
        name: 'score_as_integer',
        up: async tx => {
          await tx.execute('CREATE TABLE notes_new (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER)')
          await tx.execute('INSERT INTO notes_new SELECT id, body, CAST(score AS INTEGER) FROM notes')
          await tx.execute('DROP TABLE notes')
          await tx.execute('ALTER TABLE notes_new RENAME TO notes')
          await tx.execute('DELETE FROM notes WHERE id = 1')
        },
      },
    ])

    expect(changesOf(workspace.path('function-copy')).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['DELETE', '1'],
    ])
  })

  it('keeps capturing when a rollback copies the table back', async () => {
    const db = await workspace.open('rollback-copy')
    await seedNotes(db)
    const migration = {
      version: 1,
      name: 'score_as_integer',
      up: COPY_KEEPING_COLUMNS,
      down: `
CREATE TABLE notes_old (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score TEXT);
INSERT INTO notes_old SELECT id, body, CAST(score AS TEXT) FROM notes;
DROP TABLE notes;
ALTER TABLE notes_old RENAME TO notes;
`,
    }

    await db.migrate([migration])
    await db.rollback([migration])
    await db.execute("INSERT INTO notes VALUES (4, 'after the rollback', '9')")

    expect(changesOf(workspace.path('rollback-copy')).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '100'],
      ['INSERT', '4'],
    ])
  })
})

describe('a migration that creates more schema before it copies rows into a new table', () => {
  function loggedRowsOf(path: string, table: string): string[][] {
    return readRaw<{ operation: string; row_id: string }>(
      path,
      'SELECT operation, row_id FROM _sirannon_changes WHERE table_name = ? ORDER BY seq',
      [table],
    ).map(change => [change.operation, change.row_id])
  }

  function rowCountOf(path: string, table: string): number {
    return Number(readRaw<{ total: bigint }>(path, `SELECT COUNT(*) AS total FROM ${table}`)[0]?.total ?? -1)
  }

  it('leaves the copied rows out of the change log when the migration indexes the new table before the copy', async () => {
    const db = await workspace.open('index-before-copy')
    await seedNotes(db)

    await db.migrate([{ version: 1, name: 'score_as_integer', up: COPY_AFTER_INDEXING_THE_NEW_TABLE }])
    await db.execute("INSERT INTO notes VALUES (3, 'after the migration', 8)")

    const path = workspace.path('index-before-copy')
    expect(rowCountOf(path, 'notes')).toBe(3)
    expect(loggedRowsOf(path, 'notes')).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '3'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
  })

  it('leaves the copied rows of both tables out of the change log when the migration creates two tables before copying', async () => {
    const db = await workspace.open('two-tables-then-copy')
    await seedNotes(db)
    await db.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, note_id INTEGER NOT NULL, label TEXT NOT NULL)')
    await db.watch('tags')
    await db.execute("INSERT INTO tags VALUES (10, 1, 'draft'), (11, 2, 'review')")

    await db.migrate([{ version: 1, name: 'rebuild_notes_and_tags', up: COPY_INTO_TWO_NEW_TABLES }])
    await db.execute("INSERT INTO notes VALUES (3, 'after the migration', 8)")
    await db.execute("UPDATE tags SET label = 'published' WHERE id = 10")

    const path = workspace.path('two-tables-then-copy')
    expect(rowCountOf(path, 'notes')).toBe(3)
    expect(rowCountOf(path, 'tags')).toBe(2)
    expect(loggedRowsOf(path, 'notes')).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '3'],
    ])
    expect(loggedRowsOf(path, 'tags')).toEqual([
      ['INSERT', '10'],
      ['INSERT', '11'],
      ['UPDATE', '10'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
    expect(captureTriggersOf(path, 'tags')).toHaveLength(5)
  })

  it("captures the migration's own write after the index when the migration follows SQLite's documented rebuild order", async () => {
    const db = await workspace.open('documented-rebuild')
    await seedNotes(db)

    await db.migrate([{ version: 1, name: 'score_as_integer', up: SQLITE_DOCUMENTED_REBUILD }])

    const path = workspace.path('documented-rebuild')
    expect(rowCountOf(path, 'notes')).toBe(2)
    expect(loggedRowsOf(path, 'notes')).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['UPDATE', '2'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
  })
})

describe.each(
  PLACEMENTS,
)('a migration that turns foreign keys off to copy a watched parent table, $label', placement => {
  it('keeps the child rows, leaves the copied rows out of the change log, and captures the writes after the copy', async () => {
    const name = 'foreign-keys-off-copy'
    const db = await workspace.open(name, { writerWorker: placement.writerWorker }, placement.driver())
    await seedNotes(db)
    await db.execute(
      'CREATE TABLE tags (id INTEGER PRIMARY KEY, note_id INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE, label TEXT NOT NULL)',
    )
    await db.execute("INSERT INTO tags VALUES (10, 1, 'draft'), (11, 2, 'review')")

    await db.migrate([
      {
        version: 1,
        name: 'score_as_integer',
        up: `
PRAGMA defer_foreign_keys = ON;
PRAGMA foreign_keys = OFF;
CREATE TABLE new_notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, score INTEGER);
INSERT INTO new_notes SELECT id, body, CAST(score AS INTEGER) FROM notes;
DROP TABLE notes;
ALTER TABLE new_notes RENAME TO notes;
UPDATE notes SET body = 'written by the migration after the copy' WHERE id = 2;
PRAGMA foreign_keys = ON;
PRAGMA defer_foreign_keys = OFF;
`,
      },
    ])
    await db.execute("INSERT INTO notes VALUES (3, 'after the migration', 8)")

    const path = workspace.path(name)
    expect(readRaw<{ id: bigint; note_id: bigint }>(path, 'SELECT id, note_id FROM tags ORDER BY id')).toEqual([
      { id: 10n, note_id: 1n },
      { id: 11n, note_id: 2n },
    ])
    expect(changesOf(path).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['UPDATE', '2'],
      ['INSERT', '3'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
  })
})

describe.each(PLACEMENTS)('a migration after a restart, before anything watches the table, $label', placement => {
  async function reopenedAfterRestart(name: string): Promise<Database> {
    const first = workspace.registry()
    await seedNotes(await first.open(name, workspace.path(name)))
    await first.shutdown()
    return workspace
      .registry(placement.driver())
      .open(name, workspace.path(name), { writerWorker: placement.writerWorker })
  }

  it('drops a column of the table and keeps capturing it', async () => {
    const name = 'restart-drop'
    const reopened = await reopenedAfterRestart(name)
    await reopened.migrate([{ version: 1, name: 'drop_score', up: 'ALTER TABLE notes DROP COLUMN score' }])
    await reopened.execute("INSERT INTO notes VALUES (3, 'after the drop')")

    const changes = changesOf(workspace.path(name))
    expect(changes.map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '3'],
    ])
    expect(JSON.parse(changes[2]?.new_data ?? 'null')).toEqual({ id: 3, body: 'after the drop' })
  })

  it('copies the table and keeps capturing it', async () => {
    const name = 'restart-copy'
    const reopened = await reopenedAfterRestart(name)
    await reopened.migrate([{ version: 1, name: 'score_as_integer', up: COPY_KEEPING_COLUMNS }])
    await reopened.execute("UPDATE notes SET body = 'after the migration' WHERE id = 1")

    const path = workspace.path(name)
    expect(changesOf(path).map(change => [change.operation, change.row_id])).toEqual([
      ['INSERT', '1'],
      ['INSERT', '2'],
      ['INSERT', '100'],
      ['UPDATE', '1'],
    ])
    expect(captureTriggersOf(path, 'notes')).toHaveLength(5)
  })
})
