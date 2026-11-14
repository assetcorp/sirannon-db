import { selectQuarantined, upsertQuarantined } from '../../core/system-catalog/index.js'
import { IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL, isGroupMember } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import {
  applyCoordinatorState,
  hasCurrentPrimaryAuthorityFor,
  refreshCoordinatorState,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { restartWithFullCopy } from './full-copy-restart.js'
import { ensureAnchoredToCurrentTerm, isAnchoredToCurrentTerm } from './history-anchor.js'
import { keepCoordinatorState, removeLocalNodeFromInSyncSet } from './local-membership.js'
import { reportEngineError } from './report-error.js'

/**
 * Returns true when a group state shows this replica as repairing or faulted, so that it rejoins through the repair
 * or quarantine path.
 *
 * @param engine - The replica's engine.
 * @param state - The group state to check.
 * @returns False for the current primary.
 *
 * @internal
 */
export function requiresCoordinatorRejoin(engine: ReplicationEngine, state: ReplicationGroupState): boolean {
  if (state.currentPrimary?.nodeId === engine.nodeId) return false
  return state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)
}

/**
 * Returns true when a group state shows this replica as repairing and not faulted, so that it rebuilds through a full
 * copy.
 *
 * @param engine - The replica's engine.
 * @param state - The group state to check, or null when the node holds none.
 * @returns False for the current primary and when the state is null.
 *
 * @internal
 */
export function requiresCoordinatorRejoinSync(engine: ReplicationEngine, state: ReplicationGroupState | null): boolean {
  if (!state || state.currentPrimary?.nodeId === engine.nodeId) return false
  if (state.faultedNodeIds.includes(engine.nodeId)) return false
  return state.repairingNodeIds.includes(engine.nodeId)
}

/**
 * Records in this node's database that it holds quarantined writes, once a group state shows the node as faulted.
 * The node writes the record whenever it finds itself faulted, so that the record exists after a fault that an
 * operator sets and after a crash between the coordinator's fault and the node's first write of the record.
 *
 * @param engine - The node's engine.
 * @param state - The group state that the node read.
 * @returns A promise that settles once the record is written, or at once when the node is not faulted or already
 * holds the record.
 *
 * @internal
 */
export async function recordQuarantineIfFaulted(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<void> {
  const rejoin = engine.rejoin
  if (rejoin.quarantineRecorded || !state.faultedNodeIds.includes(engine.nodeId)) return
  rejoin.quarantineRecorded = true
  try {
    await engine.history.exclusive(() => upsertQuarantined(engine.writerConn, true))
  } catch (err: unknown) {
    rejoin.quarantineRecorded = false
    throw err
  }
}

/**
 * Prepares a restarted node's rejoin before it connects to its peers. The node loads its quarantine record, and writes
 * the record when the group shows that an operator faulted it.
 *
 * @param engine - The node's engine.
 * @returns A promise that settles once the node has prepared its rejoin.
 *
 * @internal
 */
export async function prepareCoordinatorRejoinIfNeeded(engine: ReplicationEngine): Promise<void> {
  if (!engine.config.coordinator) return
  engine.rejoin.quarantineRecorded = await selectQuarantined(engine.writerConn)
  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!state) return
  await recordQuarantineIfFaulted(engine, state)
}

/**
 * Asks the coordinator to admit this replica to the in-sync set once it has applied the current primary's log up to
 * the durability point and reconciled its history with that primary's. A repairing replica asks only after it has
 * finished, from the current primary, the full copy that rejoins it in the current term, and a draining replica leaves
 * the in-sync set. From feature level 2, the replica only clears its repairing flag after that copy, and the primary
 * admits it from its acknowledgements.
 *
 * @param engine - The replica's engine.
 * @returns A promise that settles once the coordinator has answered, or at once when the replica is not ready.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function markCoordinatorSyncReady(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!config || !state || state.currentPrimary?.nodeId === engine.nodeId) return
  if (!isGroupMember(state, engine.nodeId)) return
  if (state.faultedNodeIds.includes(engine.nodeId)) return

  if (state.drainingNodeIds.includes(engine.nodeId)) {
    await removeLocalNodeFromInSyncSet(engine, state, 'its departure from the in-sync set while it drains')
    return
  }

  const repairing = state.repairingNodeIds.includes(engine.nodeId)
  const sourceNodeId = state.currentPrimary?.nodeId
  if (state.inSyncNodeIds.includes(engine.nodeId) && !repairing) return keepCoordinatorState(engine, state)
  if (!sourceNodeId || engine.syncState.sourcePeerId !== sourceNodeId || !isAnchoredToCurrentTerm(engine)) {
    return keepCoordinatorState(engine, state)
  }
  const rejoinSyncCompleted = engine.rejoin.copiedTerm === state.primaryTerm
  if (repairing && !rejoinSyncCompleted) return keepCoordinatorState(engine, state)

  const primaryAdmits = state.featureLevel >= IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL
  if (primaryAdmits && !repairing) return keepCoordinatorState(engine, state)
  const appliedSeq = await engine.log.getLastAppliedSeq(sourceNodeId)
  if (appliedSeq < state.durabilityPointSeq) return keepCoordinatorState(engine, state)

  const admitted = await config.coordinator.admitNodeToInSyncSet({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    sourceNodeId,
    appliedSeq,
    expectedPrimaryTerm: state.primaryTerm,
    rejoinSyncCompleted,
    ...(primaryAdmits ? { clearRepairingOnly: true } : {}),
  })
  const next = admitted ?? state
  applyCoordinatorState(engine, next)
  if (!next.repairingNodeIds.includes(engine.nodeId)) engine.rejoin.copiedTerm = null
}

/**
 * Admits this node to the in-sync set once the full copy that rejoins it has finished and the node serves its data,
 * which covers a drain that outlasted the copy and an admission request that failed. The current primary admits every
 * other replica from the acknowledgements that it receives over the replica's current connection.
 *
 * @param engine - The replication engine of the local node.
 * @returns A promise that settles once the admission attempt finishes, or at once when no finished copy awaits
 * admission.
 *
 * @internal
 */
export async function admitRejoinedNodeIfReady(engine: ReplicationEngine): Promise<void> {
  const rejoin = engine.rejoin
  const state = engine.coordinatorState
  if (rejoin.copiedTerm === null || !state) return
  if (!requiresCoordinatorRejoinSync(engine, state) || rejoin.copiedTerm !== state.primaryTerm) {
    rejoin.copiedTerm = null
    return
  }
  if (!engine.running || rejoin.admissionInFlight || state.drainingNodeIds.includes(engine.nodeId)) return
  if (engine.syncState.phase !== 'ready' || engine.syncState.sourcePeerId !== state.currentPrimary?.nodeId) return
  rejoin.admissionInFlight = true
  try {
    await markCoordinatorSyncReady(engine)
  } finally {
    rejoin.admissionInFlight = false
  }
}

function admissionSeq(engine: ReplicationEngine, state: ReplicationGroupState): bigint {
  const acknowledged = engine.inSync.acknowledgedSeq
  return acknowledged > state.durabilityPointSeq ? acknowledged : state.durabilityPointSeq
}

/**
 * Asks the coordinator, as primary, to admit a replica whose acknowledgements reach both the durability point and the
 * highest sequence that this node has acknowledged. From the moment that it asks, this node counts the replica among
 * the in-sync voters of every write that it acknowledges. The coordinator admits the replica only while this node is
 * still the primary in the term that it read, and never admits a repairing replica on its acknowledgements.
 *
 * @param engine - The primary's engine.
 * @param nodeId - The replica's node ID.
 * @param ackedSeq - The newest position in this node's log that the replica acknowledged.
 * @returns A promise that settles once the coordinator has answered, or at once when the replica does not qualify.
 *
 * @internal
 */
export async function handleCoordinatorAckProgress(
  engine: ReplicationEngine,
  nodeId: string,
  ackedSeq: bigint,
): Promise<void> {
  const config = engine.config.coordinator
  const state = engine.coordinatorState
  const pendingAdmissions = engine.inSync.pendingAdmissions
  if (!config || !state) return
  if (nodeId === engine.nodeId || pendingAdmissions.has(nodeId)) return
  if (!hasCurrentPrimaryAuthorityFor(engine, state) || engine.inSync.restartProof) return
  if (engine.history.termStartTerm !== state.primaryTerm) return
  if (state.inSyncNodeIds.includes(nodeId)) return
  if (state.repairingNodeIds.includes(nodeId) || state.faultedNodeIds.includes(nodeId)) return
  if (state.drainingNodeIds.includes(nodeId)) return
  if (!isGroupMember(state, nodeId)) return
  if (ackedSeq < admissionSeq(engine, state)) return

  pendingAdmissions.add(nodeId)
  try {
    const admitted = await config.coordinator.admitNodeToInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      nodeId,
      sourceNodeId: engine.nodeId,
      appliedSeq: ackedSeq,
      expectedPrimaryTerm: state.primaryTerm,
    })
    if (admitted) applyCoordinatorState(engine, admitted)
  } finally {
    pendingAdmissions.delete(nodeId)
    engine.peerTracker.recheckWaiters()
  }
}

/**
 * Admits, as primary, every replica whose acknowledgements over its current connection reach the durability point and
 * the highest sequence that this node has acknowledged, and that the in-sync set lacks.
 *
 * @param engine - The primary's engine.
 * @returns A promise that settles once every admission attempt finishes.
 *
 * @internal
 */
export async function reconcileInSyncSet(engine: ReplicationEngine): Promise<void> {
  if (!engine.running || engine.inSync.reconciling) return
  const state = engine.coordinatorState
  if (!state || !hasCurrentPrimaryAuthorityFor(engine, state)) return

  const pending: Array<{ nodeId: string; ackedSeq: bigint }> = []
  for (const nodeId of [...state.votingDataBearingNodeIds, ...state.pendingVoterNodeIds]) {
    if (nodeId === engine.nodeId || state.inSyncNodeIds.includes(nodeId)) continue
    const ackedSeq = engine.peerTracker.connectionAckedSeq(nodeId)
    if (ackedSeq !== undefined && ackedSeq >= admissionSeq(engine, state)) pending.push({ nodeId, ackedSeq })
  }
  if (pending.length === 0) return

  engine.inSync.reconciling = true
  try {
    for (const { nodeId, ackedSeq } of pending) {
      await handleCoordinatorAckProgress(engine, nodeId, ackedSeq)
    }
  } finally {
    engine.inSync.reconciling = false
  }
}

/**
 * Starts the full copy that rebuilds a repairing node, whether the node is waiting for its first copy or serves its
 * data. A node that is copying, that is comparing its history as a demoted primary, or that has already finished the
 * copy for the current term starts nothing.
 *
 * @param engine - The node's engine.
 * @param state - The group state that the node read.
 *
 * @internal
 */
export function startCoordinatorRejoinSyncIfReady(engine: ReplicationEngine, state: ReplicationGroupState): void {
  const rejoin = engine.rejoin
  if (!state.repairingNodeIds.includes(engine.nodeId)) rejoin.copiedTerm = null
  if (!engine.running || !engine.history.active || rejoin.syncStarting) return
  if (!requiresCoordinatorRejoinSync(engine, state) || rejoin.copiedTerm === state.primaryTerm) return
  if (engine.syncState.phase === 'syncing') return
  const sourceNodeId = state.currentPrimary?.nodeId
  if (!sourceNodeId || !engine.config.transport.peers().has(sourceNodeId)) return

  rejoin.syncStarting = true
  const rebuild = engine.syncState.phase === 'pending' ? engine.syncJoiner.initiateSync() : restartWithFullCopy(engine)
  rebuild
    .catch((err: unknown) => reportEngineError(engine, err, 'coordinator-rejoin-sync', true))
    .finally(() => {
      rejoin.syncStarting = false
    })
}

/**
 * Handles this node's demotion from primary. The node leaves the in-sync set and reconciles its history with the new
 * primary's as any replica does, which keeps every change that only this node held in its table of rolled-back
 * changes, and it rebuilds through a full copy when the group shows it as repairing.
 *
 * @param engine - The former primary's engine.
 * @param state - The group state that names the new primary.
 * @returns A promise that settles once the node has left the in-sync set and started to rejoin.
 *
 * @internal
 */
export async function handleFormerPrimaryDemotion(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<void> {
  const next = await removeLocalNodeFromInSyncSet(
    engine,
    state,
    'the departure of a demoted primary from the in-sync set',
  )
  ensureAnchoredToCurrentTerm(engine)
  startCoordinatorRejoinSyncIfReady(engine, next)
}
