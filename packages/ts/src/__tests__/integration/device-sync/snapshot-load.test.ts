import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { changeLog, createWorkspace, metaValue, pulled, readRaw, stagedCount, type Workspace } from './harness.js'

const FILES_DDL = 'CREATE TABLE files (id INTEGER PRIMARY KEY, label TEXT, payload BLOB, size INTEGER)'
const BEYOND_SAFE = 2n ** 63n - 1n

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('snapshot-load')
})

afterEach(async () => {
  await workspace.dispose()
})

async function expectRefusedWhileLoading(db: Database): Promise<void> {
  await expect(db.query('SELECT 1')).rejects.toMatchObject({ code: 'SNAPSHOT_IN_PROGRESS' })
  await expect(db.execute('INSERT INTO files (id) VALUES (99)')).rejects.toMatchObject({ code: 'SNAPSHOT_IN_PROGRESS' })
}

describe('loading a snapshot onto a device', () => {
  it('replaces the tables without writing to the change log, refuses use meanwhile, and captures writes again after', async () => {
    const db = await workspace.open('replaced')
    const file = workspace.path('replaced')
    await db.execute(FILES_DDL)
    await db.watch('files')
    await db.execute("INSERT INTO files (id, label) VALUES (1, 'local before the snapshot')")
    const port = db.deviceSync()
    await port.stagePulledChanges([pulled({ seq: 1, id: 1, txId: 'tx-1', table: 'files' })])
    const logBefore = changeLog(file)
    const payload = Buffer.from([0, 1, 254, 255])

    await port.beginSnapshotLoad(['files'])
    expect([await port.snapshotLoadPending(), stagedCount(file)]).toEqual([true, 0])
    await expectRefusedWhileLoading(db)
    await port.applySnapshotSchema([FILES_DDL])
    await port.loadSnapshotPage('files', [
      { id: 10, label: 'from the server', payload, size: BEYOND_SAFE },
      { id: 11, label: null, payload: null, size: -1 },
    ])
    await port.loadSnapshotPage('files', [])
    await port.endSnapshotLoad(['files'])

    expect(await port.snapshotLoadPending()).toBe(false)
    expect(readRaw(file, 'SELECT id, label, payload, size FROM files ORDER BY id')).toEqual([
      { id: 10n, label: 'from the server', payload, size: BEYOND_SAFE },
      { id: 11n, label: null, payload: null, size: -1n },
    ])
    expect(changeLog(file)).toEqual(logBefore)

    await db.execute("UPDATE files SET label = 'local after the snapshot' WHERE id = 10")
    const captured = changeLog(file).slice(logBefore.length)
    expect(captured.map(row => [row.row_id, row.operation, row.node_id])).toEqual([
      ['10', 'UPDATE', metaValue(file, 'node_id')],
    ])
  })

  it('keeps refusing use of the database across a restart in the middle of a load, until a load completes', async () => {
    const file = workspace.path('interrupted')
    const firstRegistry = workspace.registry()
    const first = await firstRegistry.open('interrupted', file)
    await first.execute(FILES_DDL)
    await first.watch('files')
    await first.deviceSync().beginSnapshotLoad(['files'])
    await firstRegistry.shutdown()

    const secondRegistry = workspace.registry()
    const second = await secondRegistry.open('interrupted', file)
    const port = second.deviceSync()
    expect(await port.snapshotLoadPending()).toBe(true)
    await expectRefusedWhileLoading(second)
    await port.applySnapshotSchema([FILES_DDL])
    await port.abortSnapshotLoad()
    expect(await port.snapshotLoadPending()).toBe(true)
    await expectRefusedWhileLoading(second)

    await port.beginSnapshotLoad(['files'])
    await port.applySnapshotSchema([FILES_DDL])
    await port.loadSnapshotPage('files', [{ id: 1, label: 'loaded', payload: null, size: 1 }])
    await port.endSnapshotLoad(['files'])
    expect(await second.query('SELECT id, label FROM files')).toEqual([{ id: 1, label: 'loaded' }])
  })

  it('refuses unsafe schema statements and malformed pages, and changes nothing for them', async () => {
    const db = await workspace.open('refused')
    const file = workspace.path('refused')
    const port = db.deviceSync()
    await port.applySnapshotSchema([FILES_DDL, 'CREATE INDEX files_label ON files (label)'])
    await port.applySnapshotSchema([FILES_DDL, 'CREATE INDEX files_label ON files (label)'])
    const nodeId = (await port.identity()).nodeId

    const unsafeSchemas = [
      'DROP TABLE files; DROP TABLE sqlite_master',
      'CREATE TABLE copied AS SELECT * FROM files',
      'CREATE TABLE _sirannon_shadow (id INTEGER PRIMARY KEY)',
      'DROP TABLE _sirannon_meta',
      "CREATE TABLE evil (id INTEGER DEFAULT (load_extension('x')))",
      'DELETE FROM files',
      'PRAGMA writable_schema = 1',
    ]
    for (const ddl of unsafeSchemas) {
      await expect(port.applySnapshotSchema([FILES_DDL, ddl]), ddl).rejects.toThrow()
    }

    const malformedPages: Array<[string, Record<string, unknown>[]]> = [
      ['files', [{ 'bad name!': 1 }]],
      ['files', [{ id: 1, label: 'a' }, { id: 2 }]],
      ['files', [{ id: 1 }, { id: 2, label: 'b' }]],
      ['files', [{}]],
      ['not a table', [{ id: 1 }]],
      ['_sirannon_meta', [{ key: 'device_sync_pushed_seq', value: '999' }]],
      ['sqlite_sequence', [{ name: 'files', seq: 999 }]],
    ]
    for (const [table, rows] of malformedPages) {
      await expect(port.loadSnapshotPage(table, rows), `${table} ${JSON.stringify(rows)}`).rejects.toThrow()
    }

    expect(readRaw(file, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")).toEqual([
      { name: '_sirannon_meta' },
      { name: 'files' },
    ])
    expect(readRaw(file, 'SELECT id FROM files')).toEqual([])
    expect([metaValue(file, 'node_id'), metaValue(file, 'device_sync_pushed_seq')]).toEqual([nodeId, null])
  })
})
