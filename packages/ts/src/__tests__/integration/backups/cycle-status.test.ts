import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { BackupCycleStatus } from '../../../core/backup/cycle-status.js'
import type { Database } from '../../../core/database.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  addOrders,
  controlledGroup,
  createOrders,
  expectRestoresTo,
  holdReadTransaction,
  onlyChainId,
  openCycleNode,
  truncateLogFromOutside,
} from './cycle-harness.js'
import { codeOf, createWorkspace, failureOf, filesIn, gate, type Workspace, waitFor, walBytes } from './harness.js'

const SLOW_PIECE_MS = 5
const HEX_RUN_ID = /^[0-9a-f]{16}$/

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('cycle-status')
})

afterEach(async () => {
  await workspace.dispose()
})

const CYCLE_CALLS: [string, (db: Database) => unknown][] = [
  ['backupStatus', db => db.backupStatus()],
  ['backupLocation', db => db.backupLocation()],
  ['backupChain', db => db.backupChain()],
  ['captureBackupChanges', db => db.captureBackupChanges()],
  ['verifyBackup', db => db.verifyBackup('anything.db')],
  ['backupRestorePlan', db => db.backupRestorePlan(Date.now())],
  ['backupPiecesSafeToDelete', db => db.backupPiecesSafeToDelete()],
]

describe('what a database reports about its backup cycle', () => {
  it('refuses every cycle call with BACKUP_UNSUPPORTED on a database opened without backups', async () => {
    const db = await workspace.open('plain')

    for (const [label, call] of CYCLE_CALLS) {
      const error = await failureOf(Promise.resolve().then(() => call(db)))
      expect(codeOf(error), label).toBe('BACKUP_UNSUPPORTED')
    }
  })

  it('names where its backups live, with the staging directory beside the file unless the operator names one', async () => {
    const destination = workspace.destination()
    const sirannon = workspace.registry()
    const defaulted = await sirannon.open('defaulted', workspace.path('defaulted.db'), {
      backups: { destination, intervalMs: 0, onError: () => {} },
    })
    const named = await sirannon.open('named', workspace.path('named.db'), {
      backups: {
        destination,
        intervalMs: 0,
        chainName: 'orders-chain',
        stagingDir: workspace.path('elsewhere'),
        destinationTimeoutMs: 1_234,
      },
    })

    expect(defaulted.backupLocation()).toEqual({
      destination,
      chainName: 'sirannon-backup-chain',
      stagingDir: `${workspace.path('defaulted.db')}-backup`,
    })
    expect(named.backupLocation()).toEqual({
      destination,
      chainName: 'orders-chain',
      stagingDir: workspace.path('elsewhere'),
      destinationTimeoutMs: 1_234,
    })
  })

  it('reports the turn under way with its progress, then the run it finished, and clears the progress between turns', async () => {
    const held = gate()
    const destination = workspace.destination()
    destination.faults.holdWrite = name => (name.endsWith('-full.db') ? held.promise : undefined)
    const seen: BackupCycleStatus[] = []
    const node = await openCycleNode(workspace, 'watched', {
      destination,
      backups: { pieceBytes: 4096, onProgress: () => seen.push(node.db.backupStatus()) },
    })

    try {
      await waitFor(
        () => node.db.backupStatus().progress?.remainingPages === 0,
        'the full copy to finish copying pages and wait on the destination',
      )
      const during = node.db.backupStatus()
      expect(during.running).toBe(true)
      expect(during.lastRun).toBeUndefined()
      expect(during.lastError).toBeUndefined()
      expect(during.progress).toMatchObject({ phase: 'copy', piecesWritten: 0, bytesWritten: 0 })
      expect(during.progress?.runId).toMatch(HEX_RUN_ID)
    } finally {
      held.open()
    }

    await node.db.captureBackupChanges()
    const after = node.db.backupStatus()
    expect(after.running).toBe(false)
    expect(after.progress).toBeUndefined()
    expect(after.lastRun).toEqual(node.runs.at(-1))
    expect(after.chainId).toBe(onlyChainId(destination))
    expect(seen.some(status => status.running && status.progress?.phase === 'copy')).toBe(true)
    expect(seen.every(status => status.progress?.runId === node.runs[0]?.runId)).toBe(true)
  })

  it.each([
    0, 20,
  ])('records how far a failed turn got, which chain it was extending, and how long it ran (interval %sms)', async intervalMs => {
    const destination = workspace.destination()
    const node = await openCycleNode(workspace, 'failing', { destination, backups: { pieceBytes: 512, intervalMs } })
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    const chainId = onlyChainId(destination)
    destination.faults.holdWrite = () => new Promise<void>(settle => setTimeout(settle, SLOW_PIECE_MS))
    destination.faults.refuseWrite = (name, index) => name.endsWith('.wal') && index === 1
    const startedAt = Date.now()
    await addOrders(node.db, ['refused-in-transfer'])

    if (intervalMs === 0) await failureOf(node.db.captureBackupChanges())
    else await waitFor(() => node.db.backupStatus().lastError !== undefined, 'a failure in a turn the timer took')

    const failure = node.db.backupStatus().lastError
    expect(failure?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(failure?.message).toContain('the bucket refused piece 1')
    expect(failure?.chainId).toBe(chainId)
    expect(failure?.progress).toMatchObject({ phase: 'transfer', piecesWritten: 1, bytesWritten: 512 })
    expect(failure?.durationMs).toBeGreaterThanOrEqual(SLOW_PIECE_MS)
    expect(failure?.at).toBeGreaterThanOrEqual(startedAt)
    expect(node.errors.map(error => error.message)).toContain(failure?.message)
  })

  it('records a restarted log against the chain it broke, and reports each failure once when the fresh chain fails too', async () => {
    const node = await openCycleNode(workspace, 'broken', { driver: betterSqlite3({ busyTimeout: 20 }) })
    await createOrders(node.db)
    await addOrders(node.db, ['before'])
    const reader = holdReadTransaction(node.path)
    await node.db.captureBackupChanges()
    reader.release()
    const broken = onlyChainId(node.destination)
    truncateLogFromOutside(node.path)
    await addOrders(node.db, ['after'])
    node.destination.faults.refuseWrite = name => name.endsWith('-full.db')

    await failureOf(node.db.captureBackupChanges())

    expect(node.errors.map(codeOf)).toEqual(['BACKUP_LOG_REWOUND', 'BACKUP_DESTINATION_ERROR'])
    expect(node.db.backupStatus().lastError).toMatchObject({ code: 'BACKUP_DESTINATION_ERROR', chainId: broken })
    expect(node.db.backupStatus().chainId).toBeUndefined()
  })

  it('records writes lost past maxUncapturedLogBytes over the failure that stopped the turn, and starts afresh', async () => {
    const node = await openCycleNode(workspace, 'over-the-limit', { backups: { maxUncapturedLogBytes: 1 } })
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    node.destination.faults.refuseWrite = name => name.endsWith('.wal')
    await addOrders(node.db, ['staged-and-refused'])
    await failureOf(node.db.captureBackupChanges())
    await addOrders(node.db, ['held-behind-the-refusal'])

    await failureOf(node.db.captureBackupChanges())

    expect(node.errors.map(codeOf)).toEqual([
      'BACKUP_DESTINATION_ERROR',
      'BACKUP_DESTINATION_ERROR',
      'BACKUP_CHAIN_BROKEN',
    ])
    expect(node.db.backupStatus().lastError?.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(walBytes(node.path)).toBe(0)
    expect(filesIn(node.stagingDir).filter(name => name.endsWith('.wal'))).toEqual([])

    node.destination.faults.refuseWrite = undefined
    const fresh = await node.db.captureBackupChanges()
    expect(fresh?.kind).toBe('full')
    await expectRestoresTo(workspace, node.destination, ['staged-and-refused', 'held-behind-the-refusal'], 'limit')
  })

  it('reports the turn a node passed over, and whose turn it was', async () => {
    const node = await openCycleNode(workspace, 'passed-over', {
      backups: {
        preferredNode: 'primary',
        replicationGroup: controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] }),
      },
    })

    await node.db.captureBackupChanges()
    const status = node.db.backupStatus()

    expect(status.lastSkip).toMatchObject({ reason: 'not-preferred', nodeId: 'node-b', preferredNodeId: 'node-a' })
    expect(status.lastSkip?.message).toBe(
      "Node 'node-a' takes this replication group's backups, so node 'node-b' ran none this turn.",
    )
    expect(status.lastRun).toBeUndefined()
    expect(status.chainId).toBeUndefined()
    expect(node.destination.names()).toEqual([])
  })
})
