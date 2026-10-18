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
import { expectItemsAbsentOnDisk, expectRejectsWith } from './gate-assertions.js'
import { type GateEnvironment, GROUP_ID, requireNode } from './gate-environment.js'
import { recentErrorCount, waitForRecentErrorCountAbove } from './gate-observations.js'

export interface StaleMessageRoute {
  senderNodeId: string
  receiverNodeId: string
  staleTerm: bigint
}

export async function expectStaleBatchRefused(
  environment: GateEnvironment,
  route: StaleMessageRoute,
  rowId: number,
): Promise<void> {
  const receiver = requireNode(environment, route.receiverNodeId)
  const previousStaleErrors = await recentErrorCount(receiver, 'STALE_PRIMARY')
  await requireNode(environment, route.senderNodeId).sendRawBatch(
    route.receiverNodeId,
    staleBatch(route.senderNodeId, route.staleTerm, rowId),
  )
  await waitForRecentErrorCountAbove(receiver, 'STALE_PRIMARY', previousStaleErrors, 10_000)
  await expectItemsAbsentOnDisk(environment, [route.receiverNodeId], [rowId], 2_000)
}

export async function expectStaleForwardRefused(
  environment: GateEnvironment,
  route: StaleMessageRoute,
  rowId: number,
): Promise<void> {
  await expectRejectsWith(
    requireNode(environment, route.senderNodeId).sendRawForward(
      route.receiverNodeId,
      staleForwardedTransaction(rowId, route.staleTerm),
    ),
    ['STALE_PRIMARY'],
  )
  await expectItemsAbsentOnDisk(environment, [route.receiverNodeId], [rowId], 2_000)
}

export async function expectStaleSyncRequestRefused(
  environment: GateEnvironment,
  route: StaleMessageRoute,
  rowId: number,
): Promise<void> {
  await requireNode(environment, route.senderNodeId).requestRawSync(
    route.receiverNodeId,
    staleSyncRequest(route.senderNodeId, route.staleTerm),
  )
  await expectItemsAbsentOnDisk(environment, [route.receiverNodeId], [rowId], 2_000)
}

export async function expectStaleSyncBatchRefused(
  environment: GateEnvironment,
  route: StaleMessageRoute,
  rowId: number,
): Promise<void> {
  await requireNode(environment, route.senderNodeId).sendRawSyncBatch(
    route.receiverNodeId,
    staleSyncBatch(route.staleTerm, rowId),
  )
  await expectItemsAbsentOnDisk(environment, [route.receiverNodeId], [rowId], 2_000)
}

function staleBatch(sourceNodeId: string, primaryTerm: bigint, rowId: number): ReplicationBatch {
  const changes = [staleInsertChange(sourceNodeId, rowId, 'stale-batch')]
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
  const rows = [{ id: rowId, owner: 'sync-stale', value: rowId, note: 'stale-sync-batch' }]
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
  return {
    table: 'failover_items',
    operation: 'insert',
    rowId: String(rowId),
    primaryKey: { id: rowId },
    hlc: HLC.encode(Date.now(), 0, sourceNodeId),
    txId: `${sourceNodeId}-stale-${rowId}`,
    nodeId: sourceNodeId,
    newData: { id: rowId, owner: sourceNodeId, value: rowId, note },
    oldData: null,
  }
}
