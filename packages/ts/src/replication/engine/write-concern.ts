import { arraysEqual, sameNodeIdSet } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError, StalePrimaryError, WriteConcernError } from '../errors.js'
import { DEFAULT_WRITE_CONCERN_TIMEOUT_MS } from './constants.js'
import { applyCoordinatorState, errorDetails, refreshCoordinatorState } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { assertPrimaryInTerm, updateInSyncSetFromCurrent } from './in-sync-updates.js'
import { ensurePrimaryTermStart } from './term-history.js'

const WRITE_PROGRESS_PURPOSE = 'the progress of a write that it acknowledges'

/**
 * Returns the group state after a write meets its write concern. This node keeps in the in-sync set only the voters
 * that acknowledged the write over their current connections and raises the durability point to the write's position.
 * It then admits every acknowledging voter outside the set, except a repairing or faulted voter, which rejoins through
 * its own copy. Each update takes effect only while this node is still the primary in the term in which it accepted
 * the write.
 *
 * @param engine - The engine of the primary that accepted the write.
 * @param seq - The write's position in this node's change log.
 * @param state - The group state under which this node accepted the write.
 * @returns The group state after every update, whose voters can differ from the voters in `state`.
 * @throws StalePrimaryError when the term or primary duty passes to another node before every update takes effect.
 *
 * @internal
 */
export async function updateCoordinatorProgressAfterWrite(
  engine: ReplicationEngine,
  seq: bigint,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  if (!config) return state
  const term = state.primaryTerm
  let nextState = await updateInSyncSetFromCurrent(engine, {
    purpose: WRITE_PROGRESS_PURPOSE,
    known: engine.coordinatorState ?? state,
    primaryTerm: term,
    change: base => {
      const acked = engine.peerTracker.ackedConfiguredNodeIds(seq, engine.nodeId, base.votingDataBearingNodeIds)
      const retained = base.inSyncNodeIds.filter(nodeId => acked.includes(nodeId))
      if (arraysEqual(retained, base.inSyncNodeIds) && base.durabilityPointSeq >= seq) return null
      return { inSyncNodeIds: retained, durabilityPointSeq: seq }
    },
  })

  const ackedNodeIds = engine.peerTracker.ackedConfiguredNodeIds(seq, engine.nodeId, nextState.votingDataBearingNodeIds)
  for (const nodeId of ackedNodeIds) {
    if (nodeId === engine.nodeId || nextState.inSyncNodeIds.includes(nodeId)) continue
    if (nextState.repairingNodeIds.includes(nodeId) || nextState.faultedNodeIds.includes(nodeId)) continue
    const admitted = await config.coordinator.admitNodeToInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      nodeId,
      sourceNodeId: engine.nodeId,
      appliedSeq: seq,
      expectedPrimaryTerm: term,
    })
    if (!admitted) {
      throw new CoordinatorError(
        'The coordinator holds no state for this replication group, so this node cannot admit the voters that acknowledged a write',
        errorDetails(engine, nextState),
      )
    }
    applyCoordinatorState(engine, admitted)
    assertPrimaryInTerm(engine, admitted, term, WRITE_PROGRESS_PURPOSE)
    nextState = admitted
  }
  return nextState
}

async function waitForVoters(
  engine: ReplicationEngine,
  seq: bigint,
  level: 'majority' | 'all',
  state: ReplicationGroupState,
  timeoutMs: number,
): Promise<void> {
  if (level === 'majority') {
    await engine.peerTracker.waitForConfiguredMajority(seq, engine.nodeId, state.votingDataBearingNodeIds, timeoutMs)
    return
  }
  const eligibleVoters = state.votingDataBearingNodeIds.filter(nodeId => !state.drainingNodeIds.includes(nodeId))
  await engine.peerTracker.waitForConfiguredAll(seq, engine.nodeId, eligibleVoters, timeoutMs)
}

/**
 * Returns once a write meets its write concern. In coordinator mode, a `majority` or `all` write waits for the voters
 * that the group state names, then records the write's progress with the coordinator. When that record returns other
 * voters, which happens when a voter change commits during the write, this node waits again for the returned voters
 * before it acknowledges the write.
 *
 * @param engine - The engine of the node that accepted the write.
 * @param seq - The write's position in this node's change log.
 * @param wc - The write concern, whose `timeoutMs` bounds the whole wait and defaults to 5000.
 * @throws WriteConcernError when the voters do not acknowledge the write in time.
 * @throws StalePrimaryError when this node loses primary duty, or never recorded the start of its term.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function waitForWriteConcern(
  engine: ReplicationEngine,
  seq: bigint,
  wc: { level: string; timeoutMs?: number },
): Promise<void> {
  const timeoutMs = wc.timeoutMs ?? DEFAULT_WRITE_CONCERN_TIMEOUT_MS

  if (!engine.isCoordinatorMode()) {
    if (wc.level === 'majority') {
      await engine.peerTracker.waitForMajority(seq, timeoutMs)
    } else if (wc.level === 'all') {
      await engine.peerTracker.waitForAll(seq, timeoutMs)
    }
    return
  }

  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!state) {
    throw new CoordinatorError('Cannot satisfy write concern without replication-group state')
  }
  if (engine.history.termStartTerm !== state.primaryTerm) await ensurePrimaryTermStart(engine)
  const level = wc.level
  if (level !== 'majority' && level !== 'all') return
  assertPrimaryInTerm(engine, state, state.primaryTerm, WRITE_PROGRESS_PURPOSE)
  if (engine.history.termStartTerm !== state.primaryTerm) {
    throw new StalePrimaryError(
      `This node has not recorded the start of term ${state.primaryTerm}, so it cannot prove its authority for the write`,
      errorDetails(engine, state),
    )
  }

  const deadline = Date.now() + timeoutMs
  let counted = state
  for (;;) {
    const remainingMs = deadline - Date.now()
    if (remainingMs <= 0) {
      throw new WriteConcernError(`Timed out waiting for the voters of the replication group to ACK seq ${seq}`)
    }
    await waitForVoters(engine, seq, level, counted, remainingMs)
    const recorded = await updateCoordinatorProgressAfterWrite(engine, seq, counted)
    if (sameNodeIdSet(recorded.votingDataBearingNodeIds, counted.votingDataBearingNodeIds)) return
    counted = recorded
  }
}
