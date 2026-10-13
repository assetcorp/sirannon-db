import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { addOrders, type CycleNode, changeRecords, createOrders, openCycleNode } from './cycle-harness.js'
import {
  CHAIN_LIST_NAME,
  codeOf,
  createWorkspace,
  failureOf,
  joinPieces,
  orderLabels,
  pause,
  type StoredRecord,
  sha256,
  storedChain,
  storedHeads,
  type Workspace,
} from './harness.js'

const FULL_COPY_INTERVAL_MS = 150

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('restore-planning')
})

afterEach(async () => {
  await workspace.dispose()
})

interface TwoChains {
  node: CycleNode
  older: StoredRecord[]
  newer: StoredRecord[]
  written: { label: string; at: number }[]
}

async function twoChains(): Promise<TwoChains> {
  const node = await openCycleNode(workspace, 'planned', {
    backups: { fullCopyIntervalMs: FULL_COPY_INTERVAL_MS, pieceBytes: 1024 },
  })
  await node.db.captureBackupChanges()
  await createOrders(node.db)
  const written: { label: string; at: number }[] = []
  const note = async (label: string) => {
    await addOrders(node.db, [label])
    await node.db.captureBackupChanges()
    written.push({ label, at: node.runs.at(-1)?.finishedAt ?? 0 })
    await pause(3)
  }
  await note('older-1')
  await note('older-2')
  await pause(FULL_COPY_INTERVAL_MS + 20)
  await note('newer-base')
  await note('newer-1')
  await note('newer-2')
  await note('newer-3')
  const [olderHead, newerHead] = storedHeads(node.destination)
  if (!olderHead || !newerHead) throw new Error('the destination lists fewer than two chains')
  return {
    node,
    older: storedChain(node.destination, olderHead.chainId),
    newer: storedChain(node.destination, newerHead.chainId),
    written,
  }
}

function names(records: readonly { name: string }[]): string[] {
  return records.map(record => record.name)
}

describe('planning a restore', () => {
  it('plans the newest full copy at or before the moment and every change piece of its chain up to it, as a restore applies', async () => {
    const { node, older, newer } = await twoChains()
    const olderBase = older[0]
    const newerBase = newer[0]
    if (!olderBase?.finishedAt || !newerBase?.finishedAt) throw new Error('a chain holds no full copy')
    const newerChanges = changeRecords(newer)
    const olderChanges = changeRecords(older)
    const cases: [string, number, StoredRecord[], string[]][] = [
      [
        'a moment inside the newer chain',
        newerChanges[0]?.capturedAt ?? 0,
        newer,
        newerChanges.slice(0, 1).map(r => r.name),
      ],
      ['a moment between the chains', newerBase.finishedAt - 1, older, names(olderChanges)],
      ['the moment the older full copy finished', olderBase.finishedAt, older, []],
      ['the newest moment', Date.now(), newer, names(newerChanges)],
    ]

    for (const [label, moment, chain, changeNames] of cases) {
      const plan = await node.db.backupRestorePlan(moment)
      const base = chain[0]
      expect(plan.chainId, label).toBe(base?.chainId)
      expect(plan.base.name, label).toBe(base?.name)
      expect(names(plan.changes), label).toEqual(changeNames)
      const lastApplied = chain.find(record => record.name === changeNames.at(-1))
      expect(plan.restoresTo, label).toBe(lastApplied?.capturedAt ?? base?.finishedAt)

      const restored = await restoreBackup({
        destination: node.destination,
        driver: betterSqlite3(),
        destPath: workspace.path(`${label.replaceAll(' ', '-')}.db`),
        moment,
      })
      expect(restored.restoresTo, label).toBe(plan.restoresTo)
      expect(restored.changesApplied, label).toBe(changeNames.length)
    }

    const tooEarly = await failureOf(node.db.backupRestorePlan(olderBase.finishedAt - 1))
    expect(codeOf(tooEarly)).toBe('BACKUP_CHAIN_BROKEN')
    expect(tooEarly.message).toContain(new Date(olderBase.finishedAt).toISOString())
  })

  it('names the change piece missing before the moment, and plans past nothing it lacks', async () => {
    const { node, newer } = await twoChains()
    const newerChanges = changeRecords(newer)
    const chainId = newer[0]?.chainId ?? ''
    node.destination.remove(`${CHAIN_LIST_NAME}.${chainId}`, 2)

    const broken = await failureOf(node.db.backupRestorePlan(Date.now()))
    const beforeGap = await node.db.backupRestorePlan(newerChanges[0]?.capturedAt ?? 0)

    expect(codeOf(broken)).toBe('BACKUP_CHAIN_BROKEN')
    expect(broken.message).toContain(`Chain '${chainId}' is missing change piece 2`)
    expect(names(beforeGap.changes)).toEqual([newerChanges[0]?.name])
  })
})

describe('deciding which backups are safe to delete', () => {
  it('lists records only where no restore inside the window needs them, deletes none, and leaves every kept moment restorable', async () => {
    const { node, older, newer, written } = await twoChains()
    const newerFinishedAt = newer[0]?.finishedAt
    if (newerFinishedAt === undefined) throw new Error('the newer chain holds no full copy')
    const namesBefore = node.destination.names()

    expect(await node.db.backupPiecesSafeToDelete()).toEqual([])
    expect(await node.db.backupPiecesSafeToDelete({ restorableFrom: newerFinishedAt - 1 })).toEqual([])
    const superseded = await node.db.backupPiecesSafeToDelete({ restorableFrom: newerFinishedAt })
    expect(names(superseded)).toEqual(names(older))
    expect(node.destination.names()).toEqual(namesBefore)

    for (const record of superseded) {
      for (const index of node.destination.indices(record.name)) node.destination.remove(record.name, index)
    }
    const restored = workspace.path('after-deleting.db')
    await restoreBackup({
      destination: node.destination,
      driver: betterSqlite3(),
      destPath: restored,
      moment: newerFinishedAt,
    })
    expect(orderLabels(restored)).toEqual(
      written.filter(entry => entry.at <= newerFinishedAt).map(entry => entry.label),
    )
  })

  it('lists the change pieces of a chain whose full copy is gone, and those after a gap', async () => {
    const { node, older, newer } = await twoChains()
    node.destination.remove(`${CHAIN_LIST_NAME}.${older[0]?.chainId}`, 0)
    node.destination.remove(`${CHAIN_LIST_NAME}.${newer[0]?.chainId}`, 2)

    const unusable = await node.db.backupPiecesSafeToDelete()

    expect(names(unusable)).toEqual([
      ...names(changeRecords(older)),
      ...names(changeRecords(newer).filter(record => (record.sequence ?? 0) > 2)),
    ])
  })
})

describe('checking a stored backup before it is needed', () => {
  it('reads a full copy and a change piece back and reports the pieces, the bytes, and the SHA-256 that the store holds', async () => {
    const { node, newer } = await twoChains()
    const base = newer[0]
    const change = changeRecords(newer).at(-1)
    if (!base || !change) throw new Error('the newer chain holds too few records')

    for (const record of [base, change]) {
      const result = await node.db.verifyBackup(record.name)
      const joined = joinPieces(node.destination, record.name, record.pieceBytes)
      expect(result).toEqual({
        name: record.name,
        chainId: record.chainId,
        kind: record.kind,
        pieceCount: node.destination.indices(record.name).length,
        bytesRead: joined.byteLength,
        fingerprint: sha256(joined),
      })
    }
  })

  it('refuses a backup with an altered, lost, stale, or short piece, and a name no chain records', async () => {
    const { node, newer } = await twoChains()
    const base = newer[0]
    if (!base) throw new Error('the newer chain holds no full copy')
    const last = base.pieceCount - 1
    const original = (index: number) => node.destination.stored(base.name, index)
    const damage: [string, () => () => void][] = [
      ['an altered piece', () => replaced(1, flipped(original(1)))],
      [
        'a lost piece',
        () => {
          const kept = original(1)
          node.destination.remove(base.name, 1)
          return () => node.destination.replace(base.name, 1, kept)
        },
      ],
      [
        'a stale piece past the end',
        () => {
          node.destination.replace(base.name, last + 1, Buffer.alloc(base.pieceBytes))
          return () => node.destination.remove(base.name, last + 1)
        },
      ],
      ['a short last piece', () => replaced(last, original(last).subarray(0, -1))],
    ]
    function replaced(index: number, bytes: Buffer): () => void {
      const kept = original(index)
      node.destination.replace(base?.name ?? '', index, bytes)
      return () => node.destination.replace(base?.name ?? '', index, kept)
    }

    for (const [label, apply] of damage) {
      const undo = apply()
      const error = await failureOf(node.db.verifyBackup(base.name))
      undo()
      expect(codeOf(error), label).toBe('BACKUP_DESTINATION_ERROR')
    }
    const unknown = await failureOf(node.db.verifyBackup('sirannon-backup-never-written.db'))
    expect(codeOf(unknown)).toBe('BACKUP_CHAIN_BROKEN')
    expect((await node.db.verifyBackup(base.name)).bytesRead).toBe(base.bytesWritten)
  })

  it('checks only the listing and the byte count of a backup written without a fingerprint', async () => {
    const node = await openCycleNode(workspace, 'unprinted', { backups: { fingerprint: false, pieceBytes: 1024 } })
    await node.db.captureBackupChanges()
    const base = storedChain(node.destination, storedHeads(node.destination)[0]?.chainId ?? '')[0]
    if (!base) throw new Error('the chain holds no full copy')

    node.destination.replace(base.name, 0, flipped(node.destination.stored(base.name, 0)))
    const altered = await node.db.verifyBackup(base.name)
    node.destination.replace(base.name, 0, node.destination.stored(base.name, 0).subarray(0, -1))
    const short = await failureOf(node.db.verifyBackup(base.name))

    expect(altered.fingerprint).toBeUndefined()
    expect(altered.bytesRead).toBe(base.bytesWritten)
    expect(codeOf(short)).toBe('BACKUP_DESTINATION_ERROR')
  })

  it('stops a check whose destination hangs once destinationTimeoutMs passes', async () => {
    const node = await openCycleNode(workspace, 'hung-check', { backups: { destinationTimeoutMs: 150 } })
    await node.db.captureBackupChanges()
    const base = storedChain(node.destination, storedHeads(node.destination)[0]?.chainId ?? '')[0]
    node.destination.faults.holdRead = name => (name === base?.name ? new Promise<void>(() => {}) : undefined)

    const error = await failureOf(node.db.verifyBackup(base?.name ?? ''))

    expect(codeOf(error)).toBe('BACKUP_DESTINATION_ERROR')
    expect(error.message).toContain('within 150ms')
  })
})

function flipped(bytes: Buffer): Buffer {
  const altered = Buffer.from(bytes)
  altered[0] = (altered[0] ?? 0) ^ 0xff
  return altered
}
