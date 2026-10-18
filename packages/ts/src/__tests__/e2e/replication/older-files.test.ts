import { describe, expect, it } from 'vitest'
import { columnNames, readRows } from '../lib/replication-scene.js'
import { converged, PRIMARY, REPLICA, ready, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const USERS_SCHEMA = 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
const NARROW_CHANGE_LOG = `CREATE TABLE "_sirannon_changes" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT
)`
const NARROW_INSERT_TRIGGER = `CREATE TRIGGER "_sirannon_trg_users_insert" AFTER INSERT ON "users" BEGIN
  INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data)
  VALUES ('users', 'INSERT', NEW.id, json_object('id', NEW.id, 'name', NEW.name));
END`

describe('a database file whose change log predates replication', () => {
  it('gains the replication columns and indexes when it becomes a primary, keeping its rows, and replicates later writes', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [
        USERS_SCHEMA,
        NARROW_CHANGE_LOG,
        NARROW_INSERT_TRIGGER,
        "INSERT INTO users (id, name) VALUES (1, 'Ama')",
      ],
    })

    expect(columnNames(primary.dbPath, '_sirannon_changes')).toEqual(
      expect.arrayContaining(['seq', 'table_name', 'operation', 'row_id', 'node_id', 'tx_id', 'hlc']),
    )
    expect(readRows(primary.dbPath, 'SELECT seq, table_name, row_id FROM _sirannon_changes ORDER BY seq')).toEqual([
      { seq: 1, table_name: 'users', row_id: '1' },
    ])
    expect(
      readRows(
        primary.dbPath,
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '_sirannon_changes' AND name LIKE 'idx_%' ORDER BY name",
      ).map(row => row.name),
    ).toEqual(
      expect.arrayContaining([
        'idx__sirannon_changes_changed_at',
        'idx__sirannon_changes_node_id',
        'idx__sirannon_changes_hlc',
      ]),
    )

    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    await primary.engine.execute("INSERT INTO users (id, name) VALUES (2, 'Kweku')")

    await converged(primary, [replica], 'users', [
      { id: 1, name: 'Ama' },
      { id: 2, name: 'Kweku' },
    ])
  })
})
