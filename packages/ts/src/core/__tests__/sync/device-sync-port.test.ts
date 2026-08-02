import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { CHANGES_TABLE } from '../../internal-tables.js'
import { computeChecksum } from '../../sync/checksum.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
let db: Database | undefined

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-device-port-'))
})

afterEach(async () => {
  await db?.close()
  db = undefined
  rmSync(tempDir, { recursive: true, force: true })
})

async function openWatched(name: string): Promise<Database> {
  const database = await Database.create('device', join(tempDir, name), testDriver)
  await database.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await database.watch('notes')
  return database
}

describe('device sync port', () => {
  it('reports a persistent 32-hex device identity', async () => {
    db = await openWatched('identity.db')
    const first = (await db.deviceSync().identity()).nodeId
    expect(first).toMatch(/^[0-9a-f]{32}$/)
    await db.close()

    db = await Database.create('device', join(tempDir, 'identity.db'), testDriver)
    expect((await db.deviceSync().identity()).nodeId).toBe(first)
  })

  it('reads outbox batches of own stamped changes with a valid checksum', async () => {
    db = await openWatched('outbox.db')
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'one')")
    await db.execute("INSERT INTO notes (id, body) VALUES (2, 'two')")

    const port = db.deviceSync()
    const { nodeId } = await port.identity()
    const batch = await port.readOutboxBatch(0n, 100)
    expect(batch).not.toBeNull()
    if (batch === null) return
    expect(batch.sourceNodeId).toBe(nodeId)
    expect(batch.changes).toHaveLength(2)
    expect(batch.changes.map(change => change.primaryKey)).toEqual([{ id: 1 }, { id: 2 }])
    expect(batch.checksum).toBe(computeChecksum(batch.changes))
    expect(batch.toSeq >= batch.fromSeq).toBe(true)

    expect(await port.readOutboxBatch(batch.toSeq, 100)).toBeNull()
  })

  it('excludes foreign-origin and DDL rows from the outbox', async () => {
    db = await openWatched('foreign.db')
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'own')")
    const port = db.deviceSync()
    await port.identity()
    await db.runCdcMaintenance(async writer => {
      const insert = await writer.prepare(
        `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
         VALUES ('notes', 'INSERT', '9', '{"id":9}', 'ffff0000ffff0000ffff0000ffff0000', 'foreign-tx', '9999')`,
      )
      await insert.run()
    })

    const batch = await port.readOutboxBatch(0n, 100)
    expect(batch).not.toBeNull()
    expect(batch?.changes.every(change => change.operation !== 'ddl')).toBe(true)
    expect(batch?.changes.every(change => change.rowId !== '9')).toBe(true)
  })

  it('counts pending outbox changes beyond a cursor', async () => {
    db = await openWatched('pending.db')
    const port = db.deviceSync()
    expect(await port.countOutboxPending(0n)).toBe(0)
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'one')")
    await db.execute("INSERT INTO notes (id, body) VALUES (2, 'two')")
    expect(await port.countOutboxPending(0n)).toBeGreaterThanOrEqual(2)

    const batch = await port.readOutboxBatch(0n, 100)
    expect(batch).not.toBeNull()
    if (batch === null) return
    expect(await port.countOutboxPending(batch.toSeq)).toBe(0)
  })

  it('persists push and pull cursors across reopen', async () => {
    db = await openWatched('cursors.db')
    const port = db.deviceSync()
    expect(await port.getPushCursor()).toBe(0n)
    expect(await port.getPullState()).toBeNull()
    await port.setPushCursor(7n)
    await port.setPullState(42n, 'epoch-a')
    await db.close()

    db = await Database.create('device', join(tempDir, 'cursors.db'), testDriver)
    const reopened = db.deviceSync()
    expect(await reopened.getPushCursor()).toBe(7n)
    expect(await reopened.getPullState()).toEqual({ seq: 42n, epoch: 'epoch-a' })
  })

  it('protects unpushed changes from retention cleanup', async () => {
    const database = await Database.create('device', join(tempDir, 'prune.db'), testDriver, { cdcRetention: 1 })
    db = database
    await database.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await database.watch('notes')
    const port = database.deviceSync()
    await database.execute("INSERT INTO notes (id, body) VALUES (1, 'pushed')")
    const pushed = await port.readOutboxBatch(0n, 1)
    expect(pushed).not.toBeNull()
    if (pushed === null) return
    await database.execute("INSERT INTO notes (id, body) VALUES (2, 'unpushed')")
    port.protectUnpushedChanges(pushed.toSeq)

    await new Promise(resolve => setTimeout(resolve, 100))
    await database.runCdcMaintenance(async writer => {
      const backdate = await writer.prepare(`UPDATE ${CHANGES_TABLE} SET changed_at = changed_at - 3600`)
      await backdate.run()
    })
    const tracker = (
      database as unknown as { cdc: { changeTracker: { cleanup(conn: unknown): Promise<number> } | null } }
    ).cdc.changeTracker
    expect(tracker).not.toBeNull()
    if (!tracker) return
    await database.runCdcMaintenance(writer => tracker.cleanup(writer))

    const seqs: bigint[] = []
    await database.runCdcMaintenance(async writer => {
      const stmt = await writer.prepare(`SELECT seq FROM ${CHANGES_TABLE} ORDER BY seq`)
      const rows = (await stmt.all()) as { seq: number | bigint }[]
      for (const row of rows) {
        seqs.push(BigInt(row.seq))
      }
    })
    expect(seqs.some(seq => seq > pushed.toSeq)).toBe(true)
    expect(seqs.every(seq => seq > pushed.toSeq)).toBe(true)
  })
})
