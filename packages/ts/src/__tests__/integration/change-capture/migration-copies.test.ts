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
