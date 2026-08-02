import { createHash } from 'node:crypto'
import { HLC } from '../../core/sync/hlc.js'
import { canonicaliseForChecksum } from '../../replication/log.js'
import type {
  ForwardedTransaction,
  ReplicationBatch,
  ReplicationChange,
  SyncBatch,
  SyncRequest,
} from '../../replication/types.js'
import { assertItemAbsent, expectRejectsWith } from './gate-assertions.js'
import { type GateEnvironment, GROUP_ID } from './gate-environment.js'
import { recentErrorCount, waitForRecentErrorCountAbove } from './gate-observations.js'
import type { FailoverNodeProcess } from './node-process.js'

export async function expectStaleBatchRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiver: FailoverNodeProcess,
  receiverNodeId: string,
  senderNodeId: string,
  rowId: number,
): Promise<void> {
  const previousStaleErrors = await recentErrorCount(receiver, 'STALE_PRIMARY')
  await sender.sendRawBatch(receiverNodeId, staleBatch(senderNodeId, 1n, rowId))
  await waitForRecentErrorCountAbove(receiver, 'STALE_PRIMARY', previousStaleErrors, 10_000)
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

export async function expectStaleForwardRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  rowId: number,
): Promise<void> {
  await expectRejectsWith(sender.sendRawForward(receiverNodeId, staleForwardedTransaction(rowId, 1n)), [
    'TRANSPORT_ERROR',
  ])
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

export async function expectStaleSyncRequestRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  senderNodeId: string,
  rowId: number,
): Promise<void> {
  await sender.requestRawSync(receiverNodeId, staleSyncRequest(senderNodeId, 1n))
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

export async function expectStaleSyncBatchRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  rowId: number,
): Promise<void> {
  await sender.sendRawSyncBatch(receiverNodeId, staleSyncBatch(1n, rowId))
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

function staleBatch(sourceNodeId: string, primaryTerm: bigint, rowId: number): ReplicationBatch {
  const change = staleInsertChange(sourceNodeId, rowId, 'stale-batch')
  const changes = [change]
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-1-1`,
    fromSeq: 1n,
    toSeq: 1n,
    hlcRange: {
      min: HLC.encode(Date.now(), 0, sourceNodeId),
      max: HLC.encode(Date.now(), 0, sourceNodeId),
    },
    changes,
    checksum: createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex'),
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleForwardedTransaction(rowId: number, primaryTerm: bigint): ForwardedTransaction {
  return {
    requestId: `stale-forward-${rowId}`,
    statements: [
      {
        sql: 'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        params: [rowId, 'forward-stale', rowId, 'stale-forward'],
      },
    ],
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleSyncRequest(joinerNodeId: string, primaryTerm: bigint): SyncRequest {
  return {
    requestId: `${joinerNodeId}-stale-sync`,
    joinerNodeId,
    completedTables: [],
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleSyncBatch(primaryTerm: bigint, rowId: number): SyncBatch {
  const rows = [
    {
      id: rowId,
      owner: 'sync-stale',
      value: rowId,
      note: 'stale-sync-batch',
    },
  ]
  return {
    requestId: 'stale-sync-batch',
    table: 'failover_items',
    batchIndex: 0,
    rows,
    checksum: createHash('sha256').update(canonicaliseForChecksum(rows)).digest('hex'),
    isLastBatchForTable: true,
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleInsertChange(sourceNodeId: string, rowId: number, note: string): ReplicationChange {
  const hlc = HLC.encode(Date.now(), 0, sourceNodeId)
  return {
    table: 'failover_items',
    operation: 'insert',
    rowId: String(rowId),
    primaryKey: { id: rowId },
    hlc,
    txId: `${sourceNodeId}-stale-${rowId}`,
    nodeId: sourceNodeId,
    newData: {
      id: rowId,
      owner: sourceNodeId,
      value: rowId,
      note,
    },
    oldData: null,
  }
}
