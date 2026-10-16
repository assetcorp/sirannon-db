import { selectQuarantined, upsertQuarantined } from '../../core/system-catalog/index.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { AuthorityError, CoordinatorError } from '../errors.js'
import {
  applyCoordinatorState,
  errorDetails,
  hasCurrentPrimaryAuthorityFor,
  refreshCoordinatorState,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { isAnchoredToCurrentTerm, restartWithFullCopy } from './history-anchor.js'
import { updateInSyncSetFromCurrent } from './in-sync-updates.js'
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
 * Prepares a restarted node's rejoin before it connects to its peers. The node loads its quarantine record, writes
 * the record when the group shows it as faulted, and compares its history as a former primary unless it is repairing
 * with the record in place, because such a node rebuilds through a full copy without another comparison.
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
  if (!requiresCoordinatorRejoin(engine, state)) return
  if (requiresCoordinatorRejoinSync(engine, state) && engine.rejoin.quarantineRecorded) return
  await evaluateFormerPrimaryHistory(engine, state)
}

function keepState(engine: ReplicationEngine, state: ReplicationGroupState): void {
  engine.coordinatorState = state
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)
}

/**
 * Asks the coordinator to admit this replica to the in-sync set once it has applied the current primary's log up to
 * the durability point and reconciled its history with that primary's. A repairing replica asks only after it has
 * finished the full copy that rejoins it in the current term, and a draining replica leaves the in-sync set.
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
  if (!state.votingDataBearingNodeIds.includes(engine.nodeId)) return
  if (state.faultedNodeIds.includes(engine.nodeId)) return

  if (state.drainingNodeIds.includes(engine.nodeId)) {
    await removeLocalNodeFromInSyncSet(engine, state, 'its departure from the in-sync set while it drains')
    return
  }

  const repairing = state.repairingNodeIds.includes(engine.nodeId)
  const sourceNodeId = state.currentPrimary?.nodeId
  if (state.inSyncNodeIds.includes(engine.nodeId) && !repairing) return keepState(engine, state)
  if (!sourceNodeId || engine.syncState.sourcePeerId !== sourceNodeId || !isAnchoredToCurrentTerm(engine)) {
    return keepState(engine, state)
  }
  const rejoinSyncCompleted = engine.rejoin.copiedTerm === state.primaryTerm
  if (repairing && !rejoinSyncCompleted) return keepState(engine, state)

  const appliedSeq = await engine.log.getLastAppliedSeq(sourceNodeId)
  if (appliedSeq < state.durabilityPointSeq) return keepState(engine, state)

  const admitted = await config.coordinator.admitNodeToInSyncSet({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    sourceNodeId,
    appliedSeq,
    expectedPrimaryTerm: state.primaryTerm,
    rejoinSyncCompleted,
  })
  const next = admitted ?? state
  applyCoordinatorState(engine, next)
  if (!next.repairingNodeIds.includes(engine.nodeId)) engine.rejoin.copiedTerm = null
}

/**
 * Admits this node to the in-sync set once the full copy that rejoins it has finished and the node serves its data,
 * which covers a drain that outlasted the copy and an admission request that failed.
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

/**
 * Asks the coordinator, as primary, to admit a replica whose acknowledgements reach the durability point. The
 * coordinator admits it only while this node is still the primary in the term that it read, and never admits a
 * repairing replica on its acknowledgements.
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
  if (!config || !state) return
  if (nodeId === engine.nodeId) return
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) return
  if (engine.history.termStartTerm !== state.primaryTerm) return
  if (state.inSyncNodeIds.includes(nodeId)) return
  if (state.repairingNodeIds.includes(nodeId) || state.faultedNodeIds.includes(nodeId)) return
  if (!state.votingDataBearingNodeIds.includes(nodeId)) return
  if (ackedSeq < state.durabilityPointSeq) return

  const admitted = await config.coordinator.admitNodeToInSyncSet({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId,
    sourceNodeId: engine.nodeId,
    appliedSeq: ackedSeq,
    expectedPrimaryTerm: state.primaryTerm,
  })
  if (!admitted) return

  applyCoordinatorState(engine, admitted)
}

/**
 * Admits, as primary, every replica whose acknowledgements reach the durability point and that the in-sync set lacks.
 *
 * @param engine - The primary's engine.
 * @returns A promise that settles once every admission attempt finishes.
 *
 * @internal
 */
export async function reconcileInSyncSet(engine: ReplicationEngine): Promise<void> {
  if (!engine.running || engine.inSyncReconciling) return
  const state = engine.coordinatorState
  if (!state || !hasCurrentPrimaryAuthorityFor(engine, state)) return

  const caughtUpNodeIds = engine.peerTracker.ackedConfiguredNodeIds(
    state.durabilityPointSeq,
    engine.nodeId,
    state.votingDataBearingNodeIds,
  )
  const pending = caughtUpNodeIds.filter(nodeId => nodeId !== engine.nodeId && !state.inSyncNodeIds.includes(nodeId))
  if (pending.length === 0) return

  engine.inSyncReconciling = true
  try {
    for (const nodeId of pending) {
      await handleCoordinatorAckProgress(engine, nodeId, state.durabilityPointSeq)
    }
  } finally {
    engine.inSyncReconciling = false
  }
}

async function removeLocalNodeFromInSyncSet(
  engine: ReplicationEngine,
  state: ReplicationGroupState | undefined,
  purpose: string,
): Promise<ReplicationGroupState> {
  const next = await updateInSyncSetFromCurrent(engine, {
    purpose,
    known: state,
    change: current =>
      current.inSyncNodeIds.includes(engine.nodeId) && current.currentPrimary?.nodeId !== engine.nodeId
        ? { inSyncNodeIds: current.inSyncNodeIds.filter(nodeId => nodeId !== engine.nodeId) }
        : null,
  })
  applyCoordinatorState(engine, next)
  return next
}

/**
 * Removes this replica from the in-sync set before it asks for a full copy, so that the group never promotes the
 * replica while the copy replaces its rows, whatever release the primary runs.
 *
 * @param engine - The replica's engine.
 * @returns A promise that settles once the coordinator shows the replica outside the in-sync set, or at once outside
 * coordinator mode and on the current primary.
 * @throws CoordinatorError when the coordinator gives no answer or holds no state for the group.
 *
 * @internal
 */
export async function leaveInSyncSetBeforeCopy(engine: ReplicationEngine): Promise<void> {
  if (!engine.isCoordinatorMode() || engine.coordinatorState?.currentPrimary?.nodeId === engine.nodeId) return
  await removeLocalNodeFromInSyncSet(engine, undefined, 'its departure from the in-sync set before a full copy')
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
  if (!engine.running || !engine.history.active || rejoin.syncStarting || rejoin.evaluatingFormerPrimary) return
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
 * Handles this node's demotion from primary. The node marks itself repairing and compares its history with the new
 * primary's, and it rebuilds through a full copy when the comparison leaves it repairing.
 *
 * @param engine - The former primary's engine.
 * @param state - The group state that names the new primary.
 * @returns A promise that settles once the node has started its rebuild or quarantined itself.
 *
 * @internal
 */
export async function handleFormerPrimaryDemotion(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<void> {
  engine.rejoin.evaluatingFormerPrimary = true
  let evaluatedState: ReplicationGroupState
  try {
    const repairingState = await markLocalNodeRepairing(engine, state)
    evaluatedState = await evaluateFormerPrimaryHistory(engine, repairingState)
  } finally {
    engine.rejoin.evaluatingFormerPrimary = false
  }
  startCoordinatorRejoinSyncIfReady(engine, evaluatedState)
}

async function markLocalNodeRepairing(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  if (!config || state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)) {
    return state
  }
  const updated = await config.coordinator.updateNodeMaintenance({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    repairing: true,
  })
  if (!updated) {
    throw new CoordinatorError(
      'The coordinator holds no state for this replication group, so this former primary cannot mark itself for repair',
    )
  }
  keepState(engine, updated)
  return updated
}

async function evaluateFormerPrimaryHistory(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  const currentPrimary = state.currentPrimary
  if (!config || !currentPrimary || state.faultedNodeIds.includes(engine.nodeId)) return state

  const localWriteSeq = await engine.log.getLocalWriteSeq()
  const currentPrimaryAckedSeq = await engine.log.getPeerAckedSeq(currentPrimary.nodeId)
  if (localWriteSeq <= currentPrimaryAckedSeq) {
    return state
  }

  let nextState = await removeLocalNodeFromInSyncSet(
    engine,
    state,
    'the removal of a former primary that holds local-only writes',
  )
  const faulted = await config.coordinator.updateNodeMaintenance({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    repairing: false,
    faulted: true,
  })
  nextState = faulted ?? nextState
  engine.coordinatorState = nextState
  engine.coordinatorAuthority = false
  await recordQuarantineIfFaulted(engine, nextState)
  engine.emitError({
    error: new AuthorityError('Former primary returned with local-only writes and was quarantined', 'AUTHORITY_LOST', {
      ...errorDetails(engine, nextState),
      localWriteSeq: localWriteSeq.toString(),
      currentPrimaryAckedSeq: currentPrimaryAckedSeq.toString(),
    }),
    operation: 'coordinator-former-primary-divergence',
    recoverable: false,
  })
  return nextState
}
