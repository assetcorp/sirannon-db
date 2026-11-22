import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createWorkspace, PLACEMENTS, readRaw, type Workspace, writeRaw } from './harness.js'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('migration-sql')
})

afterEach(async () => {
  await workspace.dispose()
})

function changes(path: string) {
  return readRaw<{ operation: string; row_id: string; new_data: string | null }>(
    path,
    'SELECT operation, row_id, new_data FROM _sirannon_changes ORDER BY seq',
  )
}

describe.each(PLACEMENTS)('migration SQL, $label', placement => {
  async function open(name: string) {
    const db = await workspace.open(name, { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT, score TEXT)')
    await db.watch('notes')
    await db.execute("INSERT INTO Notes VALUES (1, 'first', '5')")
    return db
  }

  it('refreshes capture after schema statements with comments between keywords', async () => {
    const db = await open('commented')
    await db.migrate([
      {
        version: 1,
        name: 'commented_schema',
        up: `ALTER /* schema */ TABLE 'main'.'Notes' DROP COLUMN score;
ALTER /* schema */ TABLE [Notes] ADD COLUMN title TEXT;
UPDATE Notes SET title = 'captured' WHERE id = 1;`,
      },
    ])
    expect(JSON.parse(changes(workspace.path('commented'))[1]?.new_data ?? 'null')).toEqual({
      id: 1,
      body: 'first',
      title: 'captured',
    })
  })

  it('restores capture before a write following a batched schema statement', async () => {
    const db = await open('batched')
    await db.migrate([
      {
        version: 1,
        name: 'batched_schema',
        up: async tx => {
          await tx.executeBatch('ALTER TABLE Notes ADD COLUMN title TEXT', [[]])
          await tx.execute("UPDATE Notes SET title = 'captured' WHERE id = 1")
        },
      },
    ])
    expect(JSON.parse(changes(workspace.path('batched'))[1]?.new_data ?? 'null')).toEqual({
      id: 1,
      body: 'first',
      score: '5',
      title: 'captured',
    })
  })

  it('keeps trigger bodies intact when an identifier is end', async () => {
    const db = await open('trigger-body')
    await db.migrate([
      {
        version: 1,
        name: 'audit_trigger',
        up: `CREATE TABLE audit (id INTEGER, end TEXT);
CREATE TRIGGER audit_notes AFTER INSERT ON Notes BEGIN
INSERT INTO audit (id, end) VALUES (NEW.id, CASE WHEN NEW.id > 1 THEN 'later' ELSE 'first' END);
UPDATE audit SET end = end || ';done' WHERE id = NEW.id;
END;
INSERT INTO Notes VALUES (2, 'second', '6');`,
      },
    ])
    expect(readRaw(workspace.path('trigger-body'), 'SELECT * FROM audit')).toEqual([{ id: 2n, end: 'later;done' }])
    expect(changes(workspace.path('trigger-body')).map(row => row.row_id)).toEqual(['1', '2'])
  })

  it('leaves copied rows out when CREATE TABLE quotes the schema and table identifiers', async () => {
    const db = await open('quoted')
    await db.migrate([
      {
        version: 1,
        name: 'quoted_copy',
        up: `ALTER TABLE [Notes] RENAME TO old_notes;
CREATE TABLE 'MaIn'.'Notes' (id INTEGER PRIMARY KEY, body TEXT, score INTEGER);
INSERT INTO Notes SELECT id, body, score FROM old_notes;
DROP TABLE old_notes;
INSERT INTO Notes VALUES (2, 'second', 6);`,
      },
    ])
    expect(changes(workspace.path('quoted')).map(row => row.row_id)).toEqual(['1', '2'])
  })

  it('restores capture between down migrations', async () => {
    const db = await open('down-boundary')
    const migrations = [
      { version: 1, name: 'first', up: 'SELECT 1', down: "INSERT INTO Notes VALUES (2, 'second', '6')" },
      {
        version: 2,
        name: 'second',
        up: 'SELECT 2',
        down: `DROP TABLE Notes; CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT, score TEXT)`,
      },
    ]
    await db.migrate(migrations)
    await db.rollback(migrations, 0)
    expect(changes(workspace.path('down-boundary')).map(row => row.row_id)).toEqual(['1', '2'])
  })

  it('rolls back schema and changes together when a migration fails halfway', async () => {
    const db = await open('failed')
    const path = workspace.path('failed')
    const before = readRaw(path, "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' ORDER BY name")
    await expect(
      db.migrate([
        {
          version: 1,
          name: 'failed_copy',
          up: `ALTER TABLE Notes DROP COLUMN score;
INSERT INTO Notes VALUES (2, 'discarded');
ALTER TABLE Notes RENAME TO;`,
        },
      ]),
    ).rejects.toMatchObject({ code: 'MIGRATION_ERROR' })
    expect(readRaw(path, "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' ORDER BY name")).toEqual(before)
    await db.execute("INSERT INTO Notes VALUES (3, 'kept', '7')")
    expect(changes(path).map(row => row.row_id)).toEqual(['1', '3'])
  })

  it('keeps main capture independent of a temporary table with the same name', async () => {
    const db = await open('temporary')
    await db.migrate([
      {
        version: 1,
        name: 'temporary_copy',
        up: `CREATE TEMP TABLE Notes (temporary_value TEXT);
INSERT INTO main.Notes VALUES (2, 'captured', '6');
DROP TABLE temp.Notes;`,
      },
    ])
    expect(changes(workspace.path('temporary')).map(row => row.row_id)).toEqual(['1', '2'])
  })

  it('keeps callback writes unstamped and concurrent writes outside the migration', async () => {
    const db = await open('callback')
    let enter = () => {}
    let release = () => {}
    const entered = new Promise<void>(resolve => {
      enter = resolve
    })
    const released = new Promise<void>(resolve => {
      release = resolve
    })
    const migration = db.migrate([
      {
        version: 1,
        name: 'callback_writes',
        up: async tx => {
          await tx.execute('ALTER TABLE Notes ADD COLUMN title TEXT')
          enter()
          await released
          await db.execute("INSERT INTO Notes VALUES (2, 'callback', '6', 'captured')")
          expect(await db.queryOne('SELECT body FROM Notes WHERE id = 1')).toEqual({ body: 'first' })
        },
      },
    ])
    await entered
    const writer = db.execute("INSERT INTO Notes VALUES (3, 'concurrent', '7', 'captured')")
    release()
    await Promise.all([migration, writer])
    const rows = readRaw<{ row_id: string; node_id: string }>(
      workspace.path('callback'),
      'SELECT row_id, node_id FROM _sirannon_changes ORDER BY seq',
    )
    expect(rows.map(row => row.row_id)).toEqual(['1', '2', '3'])
    expect(rows[1]?.node_id).toBe('')
    expect(rows[2]?.node_id).toMatch(/^[a-f0-9]{32}$/)
  })

  it('leaves migration writes unstamped after a restart and a later watch', async () => {
    const first = workspace.registry(placement.driver())
    const path = workspace.path('restart-stamps')
    const db = await first.open('restart-stamps', path, { writerWorker: placement.writerWorker })
    await db.execute('CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.watch('Notes')
    await first.shutdown()
    const reopened = await workspace.open(
      'restart-stamps',
      { writerWorker: placement.writerWorker },
      placement.driver(),
    )
    await reopened.migrate([{ version: 1, name: 'seed', up: "INSERT INTO Notes VALUES (1, 'migration')" }])
    await reopened.watch('Notes')
    await reopened.execute("INSERT INTO Notes VALUES (2, 'local')")
    const rows = readRaw<{ row_id: string; node_id: string }>(
      path,
      'SELECT row_id, node_id FROM _sirannon_changes ORDER BY seq',
    )
    expect(rows.map(row => row.row_id)).toEqual(['1', '2'])
    expect(rows[0]?.node_id).toBe('')
    expect(rows[1]?.node_id).toMatch(/^[a-f0-9]{32}$/)
  })

  it('leaves another change log untouched even when its trigger mentions the default log in a string', async () => {
    const first = workspace.registry(placement.driver())
    const path = workspace.path('foreign-log')
    const db = await first.open('foreign-log', path, { writerWorker: placement.writerWorker })
    await db.execute('CREATE TABLE foreign_notes (id INTEGER PRIMARY KEY)')
    await first.shutdown()
    writeRaw(
      path,
      `CREATE TABLE other_log (value TEXT);
CREATE TRIGGER _sirannon_trg_foreign_notes_insert AFTER INSERT ON foreign_notes BEGIN
INSERT INTO other_log VALUES ('INSERT INTO "_sirannon_changes"'); END;`,
    )
    const reopened = await workspace.open('foreign-log', { writerWorker: placement.writerWorker }, placement.driver())
    await reopened.migrate([
      { version: 1, name: 'foreign_schema', up: 'ALTER TABLE foreign_notes ADD COLUMN body TEXT' },
    ])
    await reopened.execute("INSERT INTO foreign_notes VALUES (1, 'kept')")
    expect(readRaw(path, 'SELECT * FROM other_log')).toEqual([{ value: 'INSERT INTO "_sirannon_changes"' }])
  })

  it('removes capture from a renamed table until the application watches its new name', async () => {
    const db = await open('renamed-away')
    await db.migrate([
      {
        version: 1,
        name: 'archive_notes',
        up: "ALTER TABLE `Notes` RENAME TO archived_notes; INSERT INTO archived_notes VALUES (2, 'archive', '6')",
      },
    ])
    await db.watch('ARCHIVED_NOTES')
    await db.execute("INSERT INTO archived_notes VALUES (3, 'watched archive', '7')")
    expect(
      readRaw(workspace.path('renamed-away'), 'SELECT table_name, row_id FROM _sirannon_changes ORDER BY seq'),
    ).toEqual([
      { table_name: 'Notes', row_id: '1' },
      { table_name: 'archived_notes', row_id: '3' },
    ])
  })

  it('upgrades older capture triggers with another SQL spelling and no conflict staging table', async () => {
    const first = workspace.registry(placement.driver())
    const path = workspace.path('old-triggers')
    const db = await first.open('old-triggers', path, { writerWorker: placement.writerWorker })
    await db.execute('CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.watch('Notes')
    await first.shutdown()
    writeRaw(
      path,
      `DROP TRIGGER _sirannon_trg_Notes_insert;
DROP TRIGGER _sirannon_trg_Notes_update;
DROP TRIGGER _sirannon_trg_Notes_delete;
DROP TRIGGER _sirannon_trg_Notes_insert_conflicts;
DROP TRIGGER _sirannon_trg_Notes_update_conflicts;
DROP TABLE _sirannon_replaced_rows;
CREATE TRIGGER _sirannon_trg_Notes_insert AFTER INSERT ON Notes BEGIN
insert into [_sirannon_changes] (table_name, operation, row_id, new_data)
VALUES ('Notes', 'INSERT', NEW.id, json_object('id', NEW.id, 'body', NEW.body)); END;`,
    )
    const reopened = await workspace.open('old-triggers', { writerWorker: placement.writerWorker }, placement.driver())
    await reopened.migrate([{ version: 1, name: 'title', up: 'ALTER TABLE Notes ADD COLUMN title TEXT' }])
    await reopened.execute("INSERT INTO Notes VALUES (1, 'kept', 'captured')")
    expect(JSON.parse(changes(path)[0]?.new_data ?? 'null')).toEqual({ id: 1, body: 'kept', title: 'captured' })
  })
})
