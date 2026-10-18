import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import { decodeHlc, encodeHlc } from '../../e2e/lib/device-sync-oracle.js'
import {
  changeLog,
  createWorkspace,
  metaValue,
  noteIds,
  pulled,
  readRaw,
  SERVER_NODE,
  stagedCount,
  type Workspace,
  writeRaw,
} from './harness.js'

const DAY_MS = 86_400_000

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('staged-pull')
})

afterEach(async () => {
  await workspace.dispose()
})

function bodies(path: string): Array<[number, string]> {
  return readRaw<{ id: bigint; body: string }>(path, 'SELECT id, body FROM notes ORDER BY id').map(row => [
    Number(row.id),
    row.body,
  ])
}

describe('staging and applying pulled changes', () => {
  it('applies each pulled transaction whole once its last change is staged, in sequence order, however the frames arrive', async () => {
    const db = await workspace.openNotes('whole')
    const file = workspace.path('whole')
    const port = db.deviceSync()
    const seen: ChangeEvent[] = []

    await port.stagePulledChanges([pulled({ seq: 3, id: 3, txId: 'tx-1' }), pulled({ seq: 1, id: 1, txId: 'tx-1' })])
    await port.stagePulledChanges([pulled({ seq: 1, id: 1, txId: 'tx-1' })])
    expect(await port.applyStagedPull(undefined, event => seen.push(event))).toBeNull()
    expect([noteIds(file), stagedCount(file)]).toEqual([[], 2])

    await port.stagePulledChanges([
      pulled({ seq: 4, id: 4, txId: 'tx-1', txEnd: true }),
      pulled({ seq: 5, id: 5, txId: 'tx-2', txEnd: true }),
      pulled({ seq: 6, id: 6, txId: 'tx-3' }),
    ])
    expect(await port.applyStagedPull(undefined, event => seen.push(event))).toBe(5n)

    expect([noteIds(file), stagedCount(file), metaValue(file, 'device_sync_pull_seq')]).toEqual([[1, 3, 4, 5], 1, '5'])
    expect(seen.map(event => [Number(event.seq), event.origin, event.txEnd === true])).toEqual([
      [1, SERVER_NODE, false],
      [3, SERVER_NODE, false],
      [4, SERVER_NODE, true],
      [5, SERVER_NODE, true],
    ])
    const applied = changeLog(file)
    expect(applied.map(row => [row.row_id, row.node_id])).toEqual([
      ['1', SERVER_NODE],
      ['3', SERVER_NODE],
      ['4', SERVER_NODE],
      ['5', SERVER_NODE],
    ])
    expect(new Set(applied.slice(0, 3).map(row => row.tx_id))).toEqual(new Set(['tx-1']))
    expect(await port.readOutboxBatch(0n, 100)).toBeNull()
  })

  it('rolls back a transaction that fails part-way, keeps it staged, and applies it on the retry', async () => {
    const db = await workspace.openNotes('retried')
    const file = workspace.path('retried')
    const port = db.deviceSync()
    await port.stagePulledChanges([
      pulled({ seq: 1, id: 1, txId: 'tx-1' }),
      pulled({ seq: 2, id: 9, txId: 'tx-1', table: 'archive', txEnd: true }),
    ])

    await expect(port.applyStagedPull()).rejects.toThrow()
    expect([noteIds(file), stagedCount(file), await port.getPullState()]).toEqual([[], 2, null])

    await db.execute('CREATE TABLE archive (id INTEGER PRIMARY KEY, body TEXT)')
    expect(await port.applyStagedPull()).toBe(2n)
    expect([noteIds(file), stagedCount(file)]).toEqual([[1], 0])
    expect(readRaw(file, 'SELECT id FROM archive')).toEqual([{ id: 9n }])
  })

  it('refuses a pulled transaction that targets an internal table or carries a schema change, and writes none of it', async () => {
    const db = await workspace.openNotes('hostile')
    const file = workspace.path('hostile')
    const port = db.deviceSync()
    const nodeId = (await port.identity()).nodeId
    const forged: ChangeEvent[][] = [
      [
        pulled({ seq: 1, id: 1, txId: 'tx-meta' }),
        {
          ...pulled({ seq: 2, id: 0, txId: 'tx-meta', table: '_sirannon_meta', txEnd: true }),
          row: { key: 'node_id', value: 'a'.repeat(32) },
        },
      ],
      [
        { ...pulled({ seq: 3, id: 0, txId: 'tx-ddl', table: 'sqlite_master' }) },
        { ...pulled({ seq: 4, id: 4, txId: 'tx-ddl', txEnd: true }), type: 'ddl' as ChangeEvent['type'] },
      ],
    ]

    for (const transaction of forged) {
      if (stagedCount(file) > 0) writeRaw(file, 'DELETE FROM _sirannon_staged_changes')
      await port.stagePulledChanges(transaction)
      await expect(port.applyStagedPull()).rejects.toThrow()
      expect([noteIds(file), metaValue(file, 'node_id'), await port.getPullState()]).toEqual([[], nodeId, null])
    }
  })

  it('recovers staged transactions after a restart, keeps the unfinished tail, and never reapplies what the cursor covers', async () => {
    const file = workspace.path('recovered')
    const firstRegistry = workspace.registry()
    const first = await firstRegistry.open('recovered', file)
    await first.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await first.watch('notes')
    await first
      .deviceSync()
      .stagePulledChanges([
        pulled({ seq: 1, id: 1, body: 'one', txId: 'tx-1' }),
        pulled({ seq: 2, id: 2, body: 'two', txId: 'tx-1', txEnd: true }),
        pulled({ seq: 3, id: 3, body: 'three', txId: 'tx-2' }),
      ])
    await firstRegistry.shutdown()

    const secondRegistry = workspace.registry()
    const second = await secondRegistry.open('recovered', file)
    await second.watch('notes')
    const seen: number[] = []
    const recovered = await second.deviceSync().recoverStagedPull(undefined, event => seen.push(Number(event.seq)))
    expect(recovered).toEqual({ resumeSeq: 3n, appliedSeq: 2n, applyError: null })
    expect([bodies(file), stagedCount(file), seen]).toEqual([
      [
        [1, 'one'],
        [2, 'two'],
      ],
      1,
      [1, 2],
    ])
    await secondRegistry.shutdown()

    writeRaw(
      file,
      `INSERT INTO _sirannon_staged_changes (seq, table_name, operation, row_id, changed_at, new_data, node_id, tx_id, hlc, tx_end)
       VALUES (2, 'notes', 'update', '2', 0, '{"id":2,"body":"applied twice"}', ?, 'tx-1', ?, 1)`,
      [SERVER_NODE, encodeHlc(Date.now() + DAY_MS, 0, SERVER_NODE)],
    )
    const thirdRegistry = workspace.registry()
    const third = await thirdRegistry.open('recovered', file)
    await third.watch('notes')
    const replayed: number[] = []
    const again = await third.deviceSync().recoverStagedPull(undefined, event => replayed.push(Number(event.seq)))
    expect(again).toEqual({ resumeSeq: 3n, appliedSeq: 2n, applyError: null })
    expect([bodies(file)[1], stagedCount(file), replayed]).toEqual([[2, 'two'], 1, []])
  })

  it('keeps a newer local row against an older pulled update or delete, and moves the clock past a pulled stamp', async () => {
    const db = await workspace.openNotes('conflicts')
    const file = workspace.path('conflicts')
    const port = db.deviceSync()
    await db.executeBatch('INSERT INTO notes (id, body) VALUES (?, ?)', [
      [1, 'local and newer'],
      [2, 'local and newer'],
    ])
    const old = encodeHlc(1_000, 0, SERVER_NODE)
    const ahead = encodeHlc(Date.now() + DAY_MS, 5, SERVER_NODE)

    await port.stagePulledChanges([
      pulled({ seq: 1, id: 1, body: 'older remote update', type: 'update', hlc: old, txId: 'tx-1', txEnd: true }),
      pulled({ seq: 2, id: 2, type: 'delete', hlc: old, txId: 'tx-2', txEnd: true }),
      pulled({ seq: 3, id: 3, body: 'stamped a day ahead', hlc: ahead, txId: 'tx-3', txEnd: true }),
    ])
    expect(await port.applyStagedPull()).toBe(3n)
    await db.execute("UPDATE notes SET body = 'local after the pull' WHERE id = 3")

    expect(bodies(file)).toEqual([
      [1, 'local and newer'],
      [2, 'local and newer'],
      [3, 'local after the pull'],
    ])
    const local = changeLog(file).filter(row => row.node_id !== SERVER_NODE)
    const latest = local[local.length - 1]
    expect(latest.hlc > ahead).toBe(true)
    expect(decodeHlc(latest.hlc)?.wallMs).toBe(decodeHlc(ahead)?.wallMs)
  })

  it('stages, applies, and writes locally at the same time without a deadlock or a lost write', async () => {
    const db = await workspace.openNotes('interleaved')
    const file = workspace.path('interleaved')
    const port = db.deviceSync()
    const work: Promise<unknown>[] = []
    for (let at = 1; at <= 40; at++) {
      work.push(port.stagePulledChanges([pulled({ seq: at, id: at, txId: `tx-${at}`, txEnd: true })]))
      work.push(db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [1_000 + at, 'local']))
      work.push(port.applyStagedPull())
    }
    await Promise.all(work)
    await port.applyStagedPull()

    expect(noteIds(file)).toEqual([
      ...Array.from({ length: 40 }, (_, at) => at + 1),
      ...Array.from({ length: 40 }, (_, at) => 1_001 + at),
    ])
    expect([stagedCount(file), metaValue(file, 'device_sync_pull_seq')]).toEqual([0, '40'])
  })
})
