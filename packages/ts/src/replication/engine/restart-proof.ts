import { selectMaxAssignedChangeSeq } from '../../core/system-catalog/index.js'
import { IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL, majorityOf } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError } from '../errors.js'
import { DEFAULT_COORDINATOR_SESSION_TTL_MS } from './constants.js'
import { applyCoordinatorState, errorDetails, hasCurrentPrimaryAuthorityFor } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import type { RestartProof } from './in-sync-state.js'
import { markLocalNodeRepairing } from './local-membership.js'
import { reportEngineError } from './report-error.js'
import { ensurePrimaryTermStart } from './term-history.js'

function localPrimary(engine: ReplicationEngine): { nodeId: string; endpoint?: string } {
  const endpoint = engine.config.coordinator?.endpoint
  return endpoint ? { nodeId: engine.nodeId, endpoint } : { nodeId: engine.nodeId }
}

function holdsPrimaryDuty(engine: ReplicationEngine, state: ReplicationGroupState): boolean {
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) return false
  return !state.repairingNodeIds.includes(engine.nodeId) && !state.faultedNodeIds.includes(engine.nodeId)
}

function otherVoterIds(engine: ReplicationEngine, state: ReplicationGroupState): string[] {
  return state.votingDataBearingNodeIds.filter(nodeId => nodeId !== engine.nodeId)
}

async function advanceOwnTerm(engine: ReplicationEngine, state: ReplicationGroupState): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const result = await config.coordinator.compareAndAdvancePrimaryTerm({
    clusterId: config.clusterId,
    groupId: config.groupId,
    expectedPrimaryTerm: state.primaryTerm,
    nextPrimary: localPrimary(engine),
  })
  if (!result.state) {
    throw new CoordinatorError(
      'The coordinator holds no state for this replication group, so this restarted primary cannot start a new term',
      errorDetails(engine, state),
    )
  }
  engine.inSync.restartProof = null
  applyCoordinatorState(engine, result.state)
  if (result.advanced) await ensurePrimaryTermStart(engine)
}

function proofOutcome(engine: ReplicationEngine, state: ReplicationGroupState, proof: RestartProof): boolean | null {
  const reportsAtOrBelow = (nodeId: string) => {
    const seq = proof.reports.get(nodeId)
    return seq !== undefined && seq <= proof.highestAssignedSeq
  }
  const reportsHigher = (nodeId: string) => {
    const seq = proof.reports.get(nodeId)
    return seq !== undefined && seq > proof.highestAssignedSeq
  }
  const voterIds = otherVoterIds(engine, state)
  const inSyncVoterIds = voterIds.filter(nodeId => state.inSyncNodeIds.includes(nodeId))
  if (inSyncVoterIds.length > 0) {
    if (inSyncVoterIds.some(reportsAtOrBelow)) return true
    if (inSyncVoterIds.every(reportsHigher)) return false
    return Date.now() < proof.deadlineMs ? null : false
  }
  const eligibleIds = voterIds.filter(
    nodeId => !state.repairingNodeIds.includes(nodeId) && !state.faultedNodeIds.includes(nodeId),
  )
  if (eligibleIds.some(reportsHigher)) return false
  const voterCount = state.votingDataBearingNodeIds.length
  const required = Math.min(voterCount - majorityOf(voterCount) + 1, eligibleIds.length)
  return eligibleIds.filter(reportsAtOrBelow).length >= required ? true : null
}

async function settleRestartProof(engine: ReplicationEngine): Promise<void> {
  const proof = engine.inSync.restartProof
  const state = engine.coordinatorState
  if (!proof || proof.settling) return
  if (!state || !holdsPrimaryDuty(engine, state)) {
    engine.inSync.restartProof = null
    return
  }
  const keepsDuty = proofOutcome(engine, state, proof)
  if (keepsDuty === null) return
  proof.settling = true
  try {
    if (keepsDuty) {
      await advanceOwnTerm(engine, state)
      return
    }
    await markLocalNodeRepairing(engine, state)
    engine.inSync.restartProof = null
  } catch (err: unknown) {
    proof.settling = false
    reportEngineError(engine, err, 'restart-proof-settle', true)
  }
}

/**
 * Starts the proof that a restarted primary owes from feature level 2 before it accepts a write: that it lost no write
 * that it acknowledged before the restart. The node keeps primary duty in a new term at once when its group recorded
 * no in-sync majority in the current term, or when the group has no other voter. Otherwise it accepts no write until
 * other voters' position reports settle the proof.
 *
 * @param engine - The restarted node's engine.
 * @param state - The group state that the node holds after its restart rules.
 * @returns A promise that settles once the proof has started, or once the node has kept primary duty in a new term.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function beginRestartProof(engine: ReplicationEngine, state: ReplicationGroupState): Promise<void> {
  if (!holdsPrimaryDuty(engine, state) || state.featureLevel < IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL) return
  const ttlMs = engine.config.coordinator?.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
  const proof: RestartProof = {
    highestAssignedSeq: await selectMaxAssignedChangeSeq(engine.writerConn),
    deadlineMs: Date.now() + ttlMs,
    reports: new Map(),
    settling: false,
  }
  engine.inSync.restartProof = proof
  if (state.inSyncMajorityTerm >= state.primaryTerm && otherVoterIds(engine, state).length > 0) return
  proof.settling = true
  try {
    await advanceOwnTerm(engine, state)
  } finally {
    proof.settling = false
  }
}

/**
 * Records one voter's position report towards the restart proof and settles the proof when the reports decide it.
 * While other in-sync voters remain, one of their reports at or below the highest sequence that this node holds keeps
 * primary duty, and higher reports from all of them mark this node repairing. Once no other voter is in sync, the node
 * keeps duty when enough voters that are neither repairing nor faulted report at or below that sequence to include one
 * that held every acknowledged write, and any higher report from them marks it repairing.
 *
 * @param engine - The restarted primary's engine.
 * @param nodeId - The voter that sent the report.
 * @param appliedSeq - The highest sequence of this node's log that the voter reports as applied.
 * @returns A promise that settles once the report is recorded, and once any outcome that it decides is recorded.
 *
 * @internal
 */
export async function noteRestartReport(engine: ReplicationEngine, nodeId: string, appliedSeq: bigint): Promise<void> {
  const proof = engine.inSync.restartProof
  const state = engine.coordinatorState
  if (!proof || !state || nodeId === engine.nodeId || !state.votingDataBearingNodeIds.includes(nodeId)) return
  proof.reports.set(nodeId, appliedSeq)
  await settleRestartProof(engine)
}

/**
 * Settles this restarted primary's proof against the group state that it holds now, which catches a change in the
 * in-sync set or in which voters can report, and marks the node repairing once other in-sync voters stay silent past
 * the coordinator session's lifetime.
 *
 * @param engine - The restarted primary's engine.
 * @returns A promise that settles once any outcome is recorded, or at once while the proof stays open.
 *
 * @internal
 */
export async function checkRestartProof(engine: ReplicationEngine): Promise<void> {
  await settleRestartProof(engine)
}
