import { CHANGES_TABLE } from '../../core/internal-tables.js'
import {
  deleteAppliedChangesAfterSeq,
  deleteColumnVersions,
  deletePeerState,
  deleteStampedChangesUpToSeq,
  insertRolledBackChangesAfterHlc,
  selectMaxChangeSeq,
  setForeignKeysEnabled,
  upsertAnchoredTerm,
  upsertCopyRequestId,
  upsertHistoryBase,
  upsertQuarantined,
} from '../../core/system-catalog/index.js'
import type { ReplicationBatch, SyncAck, SyncComplete } from '../types.js'
import type { ReplicationEngine } from './engine.js'
import { keepUnsharedChanges, rebuildStillRequested, requestHistoryPoint, startFullCopy } from './full-copy-restart.js'
import { rollBackToHistoryPoint } from './history-rollback.js'
import { initialSyncState, servesItsData } from './internal-types.js'
import { reportEngineError } from './report-error.js'
import { reportSnapshotPosition } from './sync-recovery.js'
import { ensurePrimaryTermStart, historyOrigins } from './term-history.js'

const SCHEMA_SYNC_TABLE = '__schema__'

function tracksTermHistory(engine: ReplicationEngine): boolean {
  return engine.isCoordinatorMode()
}

function awaitsFullCopy(engine: ReplicationEngine): boolean {
  const state = engine.coordinatorState
  if (!state || engine.syncState.phase !== 'ready' || state.currentPrimary?.nodeId === engine.nodeId) return false
  if (state.faultedNodeIds.includes(engine.nodeId)) return true
  return state.repairingNodeIds.includes(engine.nodeId) && engine.rejoin.copiedTerm !== state.primaryTerm
}

function reconcilesTermHistory(engine: ReplicationEngine): boolean {
  if (!tracksTermHistory(engine)) return false
  const state = engine.coordinatorState
  const history = engine.history
  if (history.heldPrimaryInAnchoredTerm && (!state || state.primaryTerm === history.anchoredTerm)) return false
  if (!state) return true
  return !state.repairingNodeIds.includes(engine.nodeId) && !state.faultedNodeIds.includes(engine.nodeId)
}

/**
 * Returns whether this replica has reconciled its history with the current primary's, which it must do before it
 * applies a batch or reports a position in that primary's log. A former primary reconciles as any replica does once
 * the group has moved to a later term. A primary in the term whose history it holds, and a repairing or faulted node,
 * rejoin through their own paths, so this check passes for them.
 *
 * @param engine - The replica's engine.
 * @returns True outside coordinator mode and for a node that rejoins through its own path, and otherwise true only for
 * the current term.
 *
 * @internal
 */
export function isAnchoredToCurrentTerm(engine: ReplicationEngine): boolean {
  if (!reconcilesTermHistory(engine)) return true
  const anchored = engine.history.anchoredTerm
  return anchored !== null && anchored === engine.coordinatorState?.primaryTerm
}

/**
 * Starts reconciling this replica's history with the current primary's when the group has moved to a term whose
 * history the replica has not reconciled: the replica asks the primary where their histories meet and keeps serving its
 * data until the answer arrives. It sends the request again when no answer arrives within `syncAckTimeoutMs`.
 *
 * A primary and a node that held primary authority in the current term start nothing, because the primary's history
 * is their own. A repairing or faulted node and a node that is still copying start nothing either, because the repair
 * and copy paths reconcile them.
 *
 * @param engine - The replica's engine.
 *
 * @internal
 */
export function ensureAnchoredToCurrentTerm(engine: ReplicationEngine): void {
  if (!engine.running || !engine.history.active || !reconcilesTermHistory(engine)) return
  const state = engine.coordinatorState
  const history = engine.history
  if (!state || history.anchoredTerm === state.primaryTerm) return
  const primaryId = state.currentPrimary?.nodeId
  if (!primaryId || primaryId === engine.nodeId) return
  if (!servesItsData(engine.syncState.phase)) return
  if (history.resumeRequest.awaitsAnswerIn(state.primaryTerm, engine.syncAckTimeoutMs)) return
  requestHistoryPoint(engine, primaryId, state.primaryTerm, false).catch((err: unknown) =>
    reportEngineError(engine, err, 'history-reconcile-request', true),
  )
}

function copyHasReplacedRows(engine: ReplicationEngine): boolean {
  if (engine.syncState.completedTables.length > 0) return true
  for (const table of engine.expectedBatchIndex.keys()) {
    if (table !== SCHEMA_SYNC_TABLE) return true
  }
  return false
}

function abandonReconcileOnPromotion(engine: ReplicationEngine): void {
  const history = engine.history
  const phase = engine.syncState.phase
  if (!history.reconciling || !engine.coordinatorAuthority) return
  if (servesItsData(phase)) {
    history.reconciling = false
    history.resumeRequest.clear()
    return
  }
  if ((phase !== 'pending' && phase !== 'syncing') || copyHasReplacedRows(engine)) return
  history.reconciling = false
  history.resumeRequest.clear()
  engine.expectedBatchIndex.clear()
  engine.syncTableDigests.clear()
  engine.syncState = initialSyncState()
  setForeignKeysEnabled(engine.writerConn, true)
    .then(() => engine.log.setSyncMeta('ready'))
    .catch((err: unknown) => reportEngineError(engine, err, 'history-reconcile-abandon', true))
}

/**
 * Keeps this node's place in the group's history current on a timer: a primary records the start of its term, and a
 * replica reconciles its history with a new primary's. A replica that becomes primary before its reconciliation
 * replaces any row stops reconciling and serves its data, because its history then becomes the group's.
 *
 * @param engine - The node's engine.
 *
 * @internal
 */
export function keepTermHistoryCurrent(engine: ReplicationEngine): void {
  if (!engine.running || !engine.isCoordinatorMode()) return
  abandonReconcileOnPromotion(engine)
  ensurePrimaryTermStart(engine).catch((err: unknown) => reportEngineError(engine, err, 'primary-term-start', true))
  ensureAnchoredToCurrentTerm(engine)
}

/**
 * Returns whether a replica may process a batch, which it may only once it has reconciled its history with the
 * history of the batch's term, and starts that reconciliation when the batch belongs to a term that it has not
 * reconciled. A faulted node, and a repairing node that has not finished the full copy for the current term, process
 * no batch while they serve their data, because the batch would join the history that they keep before the copy. A
 * primary in the term whose history it holds and a repairing node that is copying may process every batch, because
 * they rejoin through separate paths.
 *
 * @param engine - The replica's engine.
 * @param batch - The batch, which carries the term of the primary that sent it.
 * @returns True when the replica may process the batch.
 *
 * @internal
 */
export function acceptsBatchTerm(engine: ReplicationEngine, batch: ReplicationBatch): boolean {
  if (awaitsFullCopy(engine)) return false
  if (engine.history.anchoredTerm === batch.primaryTerm || !reconcilesTermHistory(engine)) return true
  ensureAnchoredToCurrentTerm(engine)
  return false
}

/**
 * Applies a batch in turn with every history rollback, and only while the replica serves its data and, unless it
 * rejoins through the former-primary, repair, or copy path, still holds the batch's term as its reconciled term.
 *
 * @param engine - The replica's engine.
 * @param batch - The batch to apply.
 * @param apply - The function that applies the batch.
 * @returns What `apply` returns, or null when the replica dropped the batch.
 *
 * @internal
 */
export function applyInAnchoredTerm<T>(
  engine: ReplicationEngine,
  batch: ReplicationBatch,
  apply: () => Promise<T>,
): Promise<T | null> {
  if (!tracksTermHistory(engine)) return apply()
  return engine.history.exclusive(async () => {
    if (!servesItsData(engine.syncState.phase)) return null
    if (engine.history.anchoredTerm !== batch.primaryTerm && reconcilesTermHistory(engine)) return null
    return apply()
  })
}

/**
 * Handles the current primary's answer to this replica's request to reconcile its history. A resumed answer makes the
 * replica discard its history after the transaction that the answer names, in one transaction, record that it has
 * applied the primary's log up to the answer's position, take the answer's term as its reconciled term, and report
 * that position to the primary. Any other answer, a resumed one that the replica cannot apply, and an answer to a
 * request that the replica sent only before a full copy make the replica keep every history change that the primary
 * can lack, stop serving its data, and ask for a full copy. When an operator has cleared the repairing flag that
 * started the replica's rebuild, the replica keeps its rows and serves them as before.
 *
 * @param engine - The replica's engine.
 * @param complete - The primary's answer.
 * @param fromPeerId - The primary's node ID.
 * @returns True when the answer belongs to this replica's pending request, and false when it belongs to another sync.
 *
 * @internal
 */
export async function handleHistoryAnswer(
  engine: ReplicationEngine,
  complete: SyncComplete,
  fromPeerId: string,
): Promise<boolean> {
  const repairGeneration = engine.rejoin.repairGeneration
  const history = engine.history
  if (!history.resumeRequest.matches(complete.requestId)) return false
  if (complete.primaryTerm !== history.resumeRequest.term) return false
  if (engine.coordinatorState?.currentPrimary?.nodeId !== fromPeerId) return false
  const keepsBeforeCopy = history.resumeRequest.keepsBeforeCopy
  history.resumeRequest.clear()
  if (
    !keepsBeforeCopy &&
    complete.resumed === true &&
    servesItsData(engine.syncState.phase) &&
    (await rollBack(engine, complete, fromPeerId))
  ) {
    history.reconciling = false
    const durabilityPointSeq = engine.coordinatorState?.durabilityPointSeq ?? 0n
    engine.appliedSeqByPeer.set(fromPeerId, complete.snapshotSeq)
    engine.highestSourceSeqSeen = durabilityPointSeq > complete.snapshotSeq ? durabilityPointSeq : complete.snapshotSeq
    engine.syncState.sourcePeerId = fromPeerId
    await reportSnapshotPosition(engine, fromPeerId, complete.snapshotSeq)
    return true
  }
  if (!(await rebuildStillRequested(engine))) return true
  if (complete.primaryTerm !== undefined) {
    await keepUnsharedChanges(
      engine,
      complete.resumed === true ? complete.historyPoint : undefined,
      complete.primaryTerm,
      repairGeneration,
    )
  }
  await startFullCopy(engine, repairGeneration)
  return true
}

/**
 * Clears this replica's pending request to reconcile its history when the current primary refuses it, such as before
 * the primary has proved its authority in the new term, so that the replica sends the request again on its next
 * check and skips waiting `syncAckTimeoutMs`.
 *
 * @param engine - The replica's engine.
 * @param ack - The acknowledgement that the peer sent.
 * @param fromPeerId - The peer's node ID.
 * @returns True when the acknowledgement refuses this replica's pending request.
 *
 * @internal
 */
export function handleHistoryRefusal(engine: ReplicationEngine, ack: SyncAck, fromPeerId: string): boolean {
  const history = engine.history
  if (ack.success !== false || !history.resumeRequest.matches(ack.requestId)) return false
  if (ack.joinerNodeId !== engine.nodeId || engine.coordinatorState?.currentPrimary?.nodeId !== fromPeerId) return false
  history.resumeRequest.clear()
  return true
}

/**
 * Handles a copy batch that answers this replica's request to reconcile its history, which a primary from an earlier
 * release sends because it reads no history transaction in the request: the replica keeps every history change that
 * the primary did not write, stops serving its data, and asks for a full copy. When an operator has cleared the
 * repairing flag that started the replica's rebuild, the replica keeps its rows and serves them as before.
 *
 * @param engine - The replica's engine.
 * @param requestId - The request ID that the batch carries.
 * @returns True when the batch answers this replica's pending request.
 *
 * @internal
 */
export async function handleHistoryCopyBatch(engine: ReplicationEngine, requestId: string): Promise<boolean> {
  const repairGeneration = engine.rejoin.repairGeneration
  const request = engine.history.resumeRequest
  if (!request.matches(requestId)) return false
  const term = request.term
  request.clear()
  if (!(await rebuildStillRequested(engine))) return true
  if (term !== null) await keepUnsharedChanges(engine, undefined, term, repairGeneration)
  await startFullCopy(engine, repairGeneration)
  return true
}

async function rollBack(engine: ReplicationEngine, complete: SyncComplete, fromPeerId: string): Promise<boolean> {
  const point = complete.historyPoint
  const term = complete.primaryTerm
  if (!point || term === undefined) return false
  try {
    if (!(await rollBackToHistoryPoint(engine, point, fromPeerId, complete.snapshotSeq, term))) return false
  } catch (err: unknown) {
    reportEngineError(engine, err, 'history-rollback', true)
    return false
  }
  engine.history.anchoredTerm = term
  engine.history.heldPrimaryInAnchoredTerm = false
  return true
}

/**
 * Records, after a verified full copy in coordinator mode, that the replica's history now starts at the copy.
 *
 * In one transaction the replica keeps, in its table of rolled-back changes, every history change stamped after the
 * copy's newest history transaction, because the copy replaced the rows that those changes wrote. It then forgets the
 * change-log history and column versions from before the copy, which no longer describe its rows, and remembers the
 * copy's newest history transaction. In the same transaction it keeps only
 * the applied positions in the source's log up to the copy, takes the copy's term as its reconciled term, and deletes
 * its quarantine record, and it records the copy's request ID, which every acknowledgement that it sends afterwards
 * carries as proof of the copy. When the group shows the replica as repairing in the copy's term, the replica also notes
 * that it has finished the copy that rejoins it, so that it can ask for admission to the in-sync set.
 *
 * @param engine - The replica's engine.
 * @param complete - The source's completion message.
 * @param fromPeerId - The source's node ID.
 *
 * @internal
 */
export async function recordCopiedHistory(
  engine: ReplicationEngine,
  complete: SyncComplete,
  fromPeerId: string,
): Promise<void> {
  const history = engine.history
  history.reconciling = false
  history.resumeRequest.clear()
  engine.rejoin.rebuildingForRepairFlag = false
  const term = complete.primaryTerm
  if (!tracksTermHistory(engine) || term === undefined) return
  const origins = historyOrigins(engine)
  const copiedThroughHlc = complete.historyPoint?.hlc ?? null
  await history.exclusive(() =>
    engine.writerConn.transaction(async tx => {
      await upsertQuarantined(tx, false)
      await insertRolledBackChangesAfterHlc(tx, CHANGES_TABLE, origins, copiedThroughHlc, term, Date.now() / 1000)
      await deleteStampedChangesUpToSeq(tx, CHANGES_TABLE, await selectMaxChangeSeq(tx, CHANGES_TABLE))
      await deleteColumnVersions(tx)
      await upsertHistoryBase(tx, complete.historyPoint ?? null)
      await deleteAppliedChangesAfterSeq(tx, fromPeerId, complete.snapshotSeq)
      await deletePeerState(tx, fromPeerId)
      await upsertAnchoredTerm(tx, term)
      await upsertCopyRequestId(tx, complete.requestId)
    }),
  )
  history.copyRequestId = complete.requestId
  engine.rejoin.quarantineRecorded = false
  const state = engine.coordinatorState
  if (state?.primaryTerm === term && state.repairingNodeIds.includes(engine.nodeId)) engine.rejoin.copiedTerm = term
  history.anchoredTerm = term
  history.heldPrimaryInAnchoredTerm = false
}
