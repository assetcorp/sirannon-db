import { randomUUID } from 'node:crypto'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import {
  deleteAppliedChangesAfterSeq,
  deleteColumnVersions,
  deletePeerState,
  deleteStampedChangesUpToSeq,
  selectHistoryBase,
  selectMaxChangeSeq,
  selectNewestHistoryChange,
  setForeignKeysEnabled,
  upsertAnchoredTerm,
  upsertHistoryBase,
  upsertQuarantined,
} from '../../core/system-catalog/index.js'
import type { ReplicationBatch, SyncAck, SyncComplete, SyncHistoryPoint } from '../types.js'
import type { ReplicationEngine } from './engine.js'
import { rollBackToHistoryPoint } from './history-rollback.js'
import { initialSyncState, servesItsData } from './internal-types.js'
import { reportEngineError } from './report-error.js'
import { reportSnapshotPosition } from './sync-recovery.js'
import { ensurePrimaryTermStart, historyOrigins } from './term-history.js'

const SCHEMA_SYNC_TABLE = '__schema__'

function tracksTermHistory(engine: ReplicationEngine): boolean {
  return engine.isCoordinatorMode()
}

function reconcilesTermHistory(engine: ReplicationEngine): boolean {
  if (!tracksTermHistory(engine) || engine.history.heldPrimaryInAnchoredTerm) return false
  const state = engine.coordinatorState
  if (!state) return true
  return !state.repairingNodeIds.includes(engine.nodeId) && !state.faultedNodeIds.includes(engine.nodeId)
}

/**
 * Returns whether this replica has reconciled its history with the current primary's, which it must do before it
 * applies a batch or reports a position in that primary's log. A former primary and a repairing or faulted node
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
 * A primary, a node that held primary authority in its reconciled term, a repairing or faulted node, and a node that
 * is still copying start nothing, because the primary's history is their own, a former primary rejoins through the
 * former-primary path, and the repair and copy paths reconcile the others.
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
  requestHistoryReconcile(engine, primaryId, state.primaryTerm).catch((err: unknown) =>
    reportEngineError(engine, err, 'history-reconcile-request', true),
  )
}

async function requestHistoryReconcile(engine: ReplicationEngine, primaryId: string, term: bigint): Promise<void> {
  const history = engine.history
  const requestId = randomUUID()
  history.reconciling = true
  history.resumeRequest.start(requestId, term)
  const tail = await selectHistoryTail(engine)
  if (!history.resumeRequest.matches(requestId)) return
  if (tail === null) {
    await restartWithFullCopy(engine)
    return
  }
  await engine.config.transport.requestSync(
    primaryId,
    engine.decorate({
      requestId,
      joinerNodeId: engine.nodeId,
      completedTables: [],
      supportsStreamVerification: true,
      historyTail: tail,
    }),
  )
}

async function selectHistoryTail(engine: ReplicationEngine): Promise<SyncHistoryPoint | null> {
  const conn = engine.writerConn
  const newest = await selectNewestHistoryChange(conn, CHANGES_TABLE, historyOrigins(engine))
  const base = await selectHistoryBase(conn)
  const tail = newest !== null && (base === null || newest.hlc > base.hlc) ? newest : base
  return tail === null ? null : { hlc: tail.hlc, txId: tail.txId }
}

/**
 * Makes a node that serves its data stop serving it and ask its source for a full copy, which replaces every row. A
 * node that is already copying keeps its copy going.
 *
 * @param engine - The node's engine.
 * @returns A promise that settles once the node has sent its sync request.
 *
 * @internal
 */
export async function restartWithFullCopy(engine: ReplicationEngine): Promise<void> {
  engine.history.resumeRequest.clear()
  if (!servesItsData(engine.syncState.phase)) return
  engine.highestSourceSeqSeen = 0n
  engine.syncState = { ...initialSyncState(), phase: 'pending' }
  await engine.log.setSyncMeta('pending')
  await engine.syncJoiner.initiateSync()
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
 * reconciled. A former primary and a repairing or faulted node may process every batch, because they rejoin through
 * their own paths.
 *
 * @param engine - The replica's engine.
 * @param batch - The batch, which carries the term of the primary that sent it.
 * @returns True when the replica may process the batch.
 *
 * @internal
 */
export function acceptsBatchTerm(engine: ReplicationEngine, batch: ReplicationBatch): boolean {
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
 * that position to the primary. Any other answer, or a resumed one that the replica cannot apply, makes the replica
 * stop serving its data and ask for a full copy.
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
  const history = engine.history
  if (!history.resumeRequest.matches(complete.requestId)) return false
  if (complete.primaryTerm !== history.resumeRequest.term) return false
  if (engine.coordinatorState?.currentPrimary?.nodeId !== fromPeerId) return false
  history.resumeRequest.clear()
  if (
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
  await restartWithFullCopy(engine)
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
 * release sends because it reads no history transaction in the request: the replica stops serving its data and asks
 * for a full copy.
 *
 * @param engine - The replica's engine.
 * @param requestId - The request ID that the batch carries.
 * @returns True when the batch answers this replica's pending request.
 *
 * @internal
 */
export async function handleHistoryCopyBatch(engine: ReplicationEngine, requestId: string): Promise<boolean> {
  if (!engine.history.resumeRequest.matches(requestId)) return false
  await restartWithFullCopy(engine)
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
 * In one transaction the replica forgets the change-log history and column versions from before the copy, which no
 * longer describe its rows, and remembers the copy's newest history transaction. In the same transaction it keeps only
 * the applied positions in the source's log up to the copy, takes the copy's term as its reconciled term, and deletes
 * its quarantine record. When the group shows the replica as repairing in the copy's term, the replica also notes
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
  const term = complete.primaryTerm
  if (!tracksTermHistory(engine) || term === undefined) return
  await history.exclusive(() =>
    engine.writerConn.transaction(async tx => {
      await upsertQuarantined(tx, false)
      await deleteStampedChangesUpToSeq(tx, CHANGES_TABLE, await selectMaxChangeSeq(tx, CHANGES_TABLE))
      await deleteColumnVersions(tx)
      await upsertHistoryBase(tx, complete.historyPoint ?? null)
      await deleteAppliedChangesAfterSeq(tx, fromPeerId, complete.snapshotSeq)
      await deletePeerState(tx, fromPeerId)
      await upsertAnchoredTerm(tx, term)
    }),
  )
  engine.rejoin.quarantineRecorded = false
  const state = engine.coordinatorState
  if (state?.primaryTerm === term && state.repairingNodeIds.includes(engine.nodeId)) engine.rejoin.copiedTerm = term
  history.anchoredTerm = term
  history.heldPrimaryInAnchoredTerm = false
}
