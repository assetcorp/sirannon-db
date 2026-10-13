import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { columnRaw, createWorkspace, DRIVER_PLACEMENTS, readRaw, type Workspace } from './harness.js'

const MIGRATIONS: Migration[] = [
  { version: 1, name: 'create_items', up: 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)' },
  { version: 2, name: 'add_item_note', up: 'ALTER TABLE items ADD COLUMN note TEXT' },
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('older-files')
})

afterEach(async () => {
  await workspace.dispose()
})

function writeFileFromBeforeChecksums(path: string): void {
  const raw = new BetterSqlite3(path)
  try {
    raw.exec(`
      CREATE TABLE _sirannon_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
      );
      INSERT INTO _sirannon_migrations (version, name) VALUES (1, 'create_items');
      CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      INSERT INTO items (name) VALUES ('kettle');
    `)
  } finally {
    raw.close()
  }
}

describe.each(DRIVER_PLACEMENTS)('a file written before migration checksums, with $label', ({
  driver,
  writerWorker,
}) => {
  it('gains the checksum column in place and applies only the migrations that it lacks, once', async () => {
    const path = workspace.path('older.db')
    writeFileFromBeforeChecksums(path)
    const first = await workspace.registry({ driver: driver() }).open('older', path, { writerWorker })
    const second = await workspace.registry({ driver: driver() }).open('older', path, { writerWorker })

    const firstResult = await first.migrate(MIGRATIONS)
    const secondResult = await second.migrate(MIGRATIONS)

    expect(firstResult.applied).toEqual([{ version: 2, name: 'add_item_note' }])
    expect(secondResult).toEqual({ applied: [], skipped: 2 })
    expect(columnRaw(path, "SELECT name FROM pragma_table_info('_sirannon_migrations') ORDER BY cid")).toEqual([
      'version',
      'name',
      'applied_at',
      'checksum',
    ])
    expect(
      readRaw(path, 'SELECT version, name, typeof(checksum) AS kind FROM _sirannon_migrations ORDER BY version'),
    ).toEqual([
      { version: 1n, name: 'create_items', kind: 'text' },
      { version: 2n, name: 'add_item_note', kind: 'text' },
    ])
    expect(readRaw(path, 'SELECT name, note FROM items')).toEqual([{ name: 'kettle', note: null }])
  })
})
