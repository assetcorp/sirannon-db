import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { CHANGES_TABLE } from '../../internal-tables.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
let db: Database | undefined

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-snapshot-apply-'))
})

afterEach(async () => {
  await db?.close()
  db = undefined
  rmSync(tempDir, { recursive: true, force: true })
})

async function openWatched(name: string): Promise<Database> {
  const database = await Database.create('snapshot', join(tempDir, name), testDriver)
  await database.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await database.watch('notes')
  return database
}

describe('snapshot apply port', () => {
  it('wipes tables without generating change-log rows and reports the pending flag', async () => {
    db = await openWatched('wipe.db')
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'local')")
    const port = db.deviceSync()
    expect(await port.snapshotLoadPending()).toBe(false)

    await port.beginSnapshotLoad(['notes'])
    expect(await port.snapshotLoadPending()).toBe(true)

    await port.loadSnapshotPage('notes', [
      { id: 10, body: 'from server' },
      { id: 11, body: 'also from server' },
    ])
    await port.endSnapshotLoad(['notes'])
    expect(await port.snapshotLoadPending()).toBe(false)

    const rows = await db.query<{ id: number }>('SELECT id FROM notes ORDER BY id')
    expect(rows.map(row => row.id)).toEqual([10, 11])

    const logRows: string[] = []
    await db.runCdcMaintenance(async writer => {
      const stmt = await writer.prepare(`SELECT row_id FROM ${CHANGES_TABLE} WHERE row_id IN ('10', '11')`)
      for (const row of (await stmt.all()) as { row_id: string }[]) {
        logRows.push(row.row_id)
      }
    })
    expect(logRows).toEqual([])
  })

  it('captures writes again after the load re-watches tables', async () => {
    db = await openWatched('rewatch.db')
    const port = db.deviceSync()
    await port.beginSnapshotLoad(['notes'])
    await port.loadSnapshotPage('notes', [{ id: 1, body: 'server' }])
    await port.endSnapshotLoad(['notes'])

    await db.execute("INSERT INTO notes (id, body) VALUES (2, 'after snapshot')")
    let captured = 0
    await db.runCdcMaintenance(async writer => {
      const stmt = await writer.prepare(`SELECT COUNT(*) AS cnt FROM ${CHANGES_TABLE} WHERE row_id = '2'`)
      const row = (await stmt.get()) as { cnt: number | bigint }
      captured = Number(row.cnt)
    })
    expect(captured).toBe(1)
  })

  it('applies snapshot schema for missing tables and rejects unsafe DDL', async () => {
    db = await Database.create('snapshot', join(tempDir, 'schema.db'), testDriver)
    const port = db.deviceSync()
    await port.applySnapshotSchema(['CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)'])
    await port.applySnapshotSchema(['CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)'])
    await db.execute("INSERT INTO tags (id, label) VALUES (1, 'ok')")

    await expect(port.applySnapshotSchema(['DROP TABLE tags; DROP TABLE sqlite_master'])).rejects.toThrow(
      /safety validation/,
    )
    await expect(port.applySnapshotSchema(['CREATE TABLE evil AS SELECT * FROM tags'])).rejects.toThrow(
      /safety validation/,
    )
  })

  it('rejects malformed snapshot pages', async () => {
    db = await openWatched('reject.db')
    const port = db.deviceSync()
    await port.beginSnapshotLoad(['notes'])

    await expect(port.loadSnapshotPage('notes', [{ 'bad name!': 1 }])).rejects.toThrow(/Invalid column name/)
    await expect(port.loadSnapshotPage('notes', [{ id: 1, body: 'a' }, { id: 2 }])).rejects.toThrow(
      /do not share one column set/,
    )
    await expect(port.loadSnapshotPage('not a table', [{ id: 1 }])).rejects.toThrow(/Invalid table name/)

    await port.endSnapshotLoad(['notes'])
  })
})
