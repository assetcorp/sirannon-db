import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { specChecksum } from '../../e2e/lib/device-sync-oracle.js'
import {
  changeLog,
  createWorkspace,
  FAST_POLL_MS,
  metaValue,
  readRaw,
  type Workspace,
  waitFor,
  writeRaw,
} from './harness.js'

const FILES_DDL = 'CREATE TABLE files (id INTEGER PRIMARY KEY, label TEXT, payload BLOB, size INTEGER)'
const FOREIGN_NODE = 'e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1'
const BEYOND_SAFE = 2n ** 62n + 7n
const HOUR_MS = 3_600_000

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('outbox')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openFiles(name: string, options?: Parameters<Workspace['open']>[1]): Promise<Database> {
  const db = await workspace.open(name, options)
  await db.execute(FILES_DDL)
  await db.watch('files')
  return db
}

function plantForeignAndDdlRows(path: string, ownNode: string): void {
  writeRaw(
    path,
    `INSERT INTO _sirannon_changes (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
     VALUES ('files', 'INSERT', '900', '{"id":900}', ?, ?, ?)`,
    [FOREIGN_NODE, 'f'.repeat(32), `${'0'.repeat(12)}-0000-${FOREIGN_NODE}`],
  )
  writeRaw(
    path,
    `INSERT INTO _sirannon_changes (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
     VALUES ('__ddl__', 'DDL', '', '{"ddlStatement":"CREATE TABLE extra (id INTEGER PRIMARY KEY)"}', ?, ?, ?)`,
    [ownNode, 'd'.repeat(32), `${'f'.repeat(12)}-0000-${ownNode}`],
  )
}

describe('the device outbox', () => {
  it('returns only the row changes that the device made, in log order, with values intact and a checksum per the spec', async () => {
    const db = await openFiles('own-changes')
    const file = workspace.path('own-changes')
    const port = db.deviceSync()
    const { nodeId } = await port.identity()
    const payload = Buffer.from([0, 255, 128, 7])
    await db.execute('INSERT INTO files (id, label, payload, size) VALUES (1, ?, ?, ?)', ['héllo 東京', payload, 42])
    plantForeignAndDdlRows(file, nodeId)
    await db.execute('UPDATE files SET size = ? WHERE id = 1', [BEYOND_SAFE])
    await db.execute('INSERT INTO files (id, label) VALUES (2, NULL)')
    await db.execute('DELETE FROM files WHERE id = 2')

    const batch = await port.readOutboxBatch(0n, 100)
    const own = changeLog(file).filter(row => row.node_id === nodeId && row.operation !== 'DDL')

    expect(batch).not.toBeNull()
    if (batch === null) return
    expect(batch.sourceNodeId).toBe(nodeId)
    expect([batch.fromSeq, batch.toSeq]).toEqual([own[0].seq, own[own.length - 1].seq])
    expect(batch.batchId).toBe(`${nodeId}-${batch.fromSeq}-${batch.toSeq}`)
    expect(batch.hlcRange).toEqual({ min: own[0].hlc, max: own[own.length - 1].hlc })
    expect(batch.checksum).toBe(specChecksum(batch.changes))
    expect(batch.changes.map(change => [change.operation, change.rowId, change.txId, change.hlc])).toEqual(
      own.map(row => [row.operation.toLowerCase(), row.row_id, row.tx_id, row.hlc]),
    )
    expect(batch.changes.map(change => [change.primaryKey, change.newData, change.oldData])).toEqual([
      [{ id: 1 }, { id: 1, label: 'héllo 東京', payload, size: 42 }, null],
      [
        { id: 1 },
        { id: 1, label: 'héllo 東京', payload, size: BEYOND_SAFE },
        { id: 1, label: 'héllo 東京', payload, size: 42 },
      ],
      [{ id: 2 }, { id: 2, label: null, payload: null, size: null }, null],
      [{ id: 2 }, null, { id: 2, label: null, payload: null, size: null }],
    ])
  })

  it('pages through the outbox at every batch size without a gap or a repeat, and counts what remains', async () => {
    const db = await openFiles('paging')
    const port = db.deviceSync()
    const { nodeId } = await port.identity()
    for (let id = 1; id <= 7; id++) await db.execute('INSERT INTO files (id, label) VALUES (?, ?)', [id, `f${id}`])
    plantForeignAndDdlRows(workspace.path('paging'), nodeId)
    await db.execute("INSERT INTO files (id, label) VALUES (8, 'after the planted rows')")
    const ownSeqs = changeLog(workspace.path('paging'))
      .filter(row => row.node_id === nodeId && row.operation !== 'DDL')
      .map(row => row.seq)

    expect(await port.countOutboxPending(0n)).toBe(8)
    for (const size of [1, 3, 8, 1_000]) {
      const seen: bigint[] = []
      let cursor = 0n
      for (let round = 0; round < 20; round++) {
        const batch = await port.readOutboxBatch(cursor, size)
        if (batch === null) break
        expect(batch.changes.length, `batch size ${size}`).toBeLessThanOrEqual(size)
        expect(batch.checksum).toBe(specChecksum(batch.changes))
        seen.push(...ownSeqs.filter(seq => seq >= batch.fromSeq && seq <= batch.toSeq))
        cursor = batch.toSeq
        expect(await port.countOutboxPending(cursor)).toBe(ownSeqs.filter(seq => seq > cursor).length)
      }
      expect(seen, `batch size ${size}`).toEqual(ownSeqs)
    }
    expect(await port.readOutboxBatch(ownSeqs[ownSeqs.length - 1], 100)).toBeNull()
    expect(await port.readOutboxBatch(0n, 0)).toBeNull()
  })

  it('keeps the push cursor, the pull cursor and its epoch, and the resync flag across reopens', async () => {
    const registry = workspace.registry()
    const file = workspace.path('cursors')
    const first = (await registry.open('cursors', file)).deviceSync()
    expect([await first.getPushCursor(), await first.getPullState(), await first.getResyncRequired()]).toEqual([
      0n,
      null,
      false,
    ])
    await first.setPushCursor(7n)
    await first.setPullState(9_223_372_036_854_775_807n, 'epoch-a')
    await first.setResyncRequired(true)
    await registry.close('cursors')

    const second = (await registry.open('cursors', file)).deviceSync()
    expect((await second.identity()).nodeId).toBe(metaValue(file, 'node_id'))
    expect([await second.getPushCursor(), await second.getPullState(), await second.getResyncRequired()]).toEqual([
      7n,
      { seq: 9_223_372_036_854_775_807n, epoch: 'epoch-a' },
      true,
    ])
    await second.setPullState(12n)
    expect(await second.getPullState()).toEqual({ seq: 12n, epoch: 'epoch-a' })
    await registry.close('cursors')

    writeRaw(file, "UPDATE _sirannon_meta SET value = '-3' WHERE key = 'device_sync_pushed_seq'")
    writeRaw(file, "UPDATE _sirannon_meta SET value = '12abc' WHERE key = 'device_sync_pull_seq'")
    const third = (await registry.open('cursors', file)).deviceSync()
    expect([await third.getPushCursor(), await third.getPullState()]).toEqual([0n, null])
  })

  it('keeps every change above the pushed position through change-log pruning', async () => {
    const db = await openFiles('retention', { cdcRetention: -HOUR_MS, cdcPollInterval: FAST_POLL_MS })
    const file = workspace.path('retention')
    db.on('files').subscribe(() => {})
    const port = db.deviceSync()
    await db.execute("INSERT INTO files (id, label) VALUES (1, 'pushed')")
    const pushed = await port.readOutboxBatch(0n, 1)
    expect(pushed).not.toBeNull()
    if (pushed === null) return
    port.protectUnpushedChanges(pushed.toSeq)
    for (let id = 2; id <= 4; id++) await db.execute('INSERT INTO files (id, label) VALUES (?, ?)', [id, 'unpushed'])

    await waitFor(() => changeLog(file).every(row => row.seq > pushed.toSeq), 'the pushed change to leave the log')
    expect(changeLog(file).map(row => row.row_id)).toEqual(['2', '3', '4'])
    expect(await port.countOutboxPending(pushed.toSeq)).toBe(3)

    port.protectUnpushedChanges(pushed.toSeq + 3n)
    await waitFor(() => changeLog(file).length === 0, 'the newly pushed changes to leave the log')
    expect(readRaw(file, 'SELECT id FROM files ORDER BY id')).toHaveLength(4)
  })

  it('keeps unpushed changes through a reopen before anything asks the outbox for them', async () => {
    const registry = workspace.registry()
    const file = workspace.path('reopened')
    const options = { cdcRetention: -HOUR_MS, cdcPollInterval: FAST_POLL_MS }
    const first = await registry.open('reopened', file, options)
    await first.execute(FILES_DDL)
    await first.watch('files')
    const firstPort = first.deviceSync()
    await first.execute("INSERT INTO files (id, label) VALUES (1, 'pushed before the restart')")
    const pushed = await firstPort.readOutboxBatch(0n, 10)
    if (pushed === null) throw new Error('the outbox returned nothing to push')
    await firstPort.setPushCursor(pushed.toSeq)
    await first.execute("INSERT INTO files (id, label) VALUES (2, 'written offline')")
    await registry.close('reopened')

    const reopened = await registry.open('reopened', file, options)
    await reopened.watch('files')
    const seen: unknown[] = []
    reopened.on('files').subscribe(event => seen.push(event))
    await reopened.execute("INSERT INTO files (id, label) VALUES (3, 'written after the restart')")
    await waitFor(() => seen.length >= 1, 'the change after the restart')
    await waitFor(() => changeLog(file).every(row => row.seq > pushed.toSeq), 'the pruning pass')

    expect(changeLog(file).map(row => row.row_id)).toEqual(['2', '3'])
    expect(await reopened.deviceSync().countOutboxPending(pushed.toSeq)).toBe(2)
  })
})
