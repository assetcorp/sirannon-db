import { selectMaxAssignedChangeSeq } from '../../core/system-catalog/index.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError } from '../errors.js'
import { DEFAULT_COORDINATOR_SESSION_TTL_MS } from './constants.js'
import { applyCoordinatorState, errorDetails, hasCurrentPrimaryAuthorityFor } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
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

async function settleRestartProof(engine: ReplicationEngine, keepsDuty: boolean): Promise<void> {
  const proof = engine.inSync.restartProof
  const state = engine.coordinatorState
  if (!proof || proof.settling) return
  if (!state || !holdsPrimaryDuty(engine, state)) {
    engine.inSync.restartProof = null
    return
  }
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
 * Starts the proof that a restarted primary owes before it accepts a write: that it lost no write that it acknowledged
 * before the restart. The node keeps primary duty at once when no other in-sync voter can hold such a write, and
 * otherwise waits for an in-sync voter's position report, accepting no write until the proof settles.
 *
 * @param engine - The restarted node's engine.
 * @param state - The group state that the node holds after its restart rules.
 * @returns A promise that settles once the proof has started, or once the node has kept primary duty in a new term.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function beginRestartProof(engine: ReplicationEngine, state: ReplicationGroupState): Promise<void> {
  if (!holdsPrimaryDuty(engine, state)) return
  const voterIds = state.inSyncNodeIds.filter(
    nodeId => nodeId !== engine.nodeId && state.votingDataBearingNodeIds.includes(nodeId),
  )
  const ttlMs = engine.config.coordinator?.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
  engine.inSync.restartProof = {
    highestAssignedSeq: await selectMaxAssignedChangeSeq(engine.writerConn),
    deadlineMs: Date.now() + ttlMs,
    voterIds,
    higherReports: new Set(),
    settling: false,
  }
  if (voterIds.length > 0) return
  engine.inSync.restartProof.settling = true
  try {
    await advanceOwnTerm(engine, state)
  } finally {
    const proof = engine.inSync.restartProof
    if (proof) proof.settling = false
  }
}

/**
 * Counts one in-sync voter's position report towards the restart proof. A report at or below the highest sequence
 * that this node holds proves that this node lost no acknowledged write, so the node keeps primary duty in a new term.
 * Once every in-sync voter reports a higher sequence, this node marks itself repairing.
 *
 * @param engine - The restarted primary's engine.
 * @param nodeId - The voter that sent the report.
 * @param appliedSeq - The highest sequence of this node's log that the voter reports as applied.
 * @returns A promise that settles once the report is counted, and once any outcome that it decides is recorded.
 *
 * @internal
 */
export async function noteRestartReport(engine: ReplicationEngine, nodeId: string, appliedSeq: bigint): Promise<void> {
  const proof = engine.inSync.restartProof
  if (!proof || proof.settling || !proof.voterIds.includes(nodeId)) return
  if (appliedSeq <= proof.highestAssignedSeq) {
    await settleRestartProof(engine, true)
    return
  }
  proof.higherReports.add(nodeId)
  if (proof.voterIds.every(voterId => proof.higherReports.has(voterId))) await settleRestartProof(engine, false)
}

/**
 * Marks this restarted primary repairing once its restart proof outlives the coordinator session's lifetime without
 * a report that proves its copy.
 *
 * @param engine - The restarted primary's engine.
 * @returns A promise that settles once any outcome is recorded, or at once while the proof has time left.
 *
 * @internal
 */
export async function checkRestartProofDeadline(engine: ReplicationEngine): Promise<void> {
  const proof = engine.inSync.restartProof
  if (!proof || proof.settling) return
  const state = engine.coordinatorState
  if (state && !holdsPrimaryDuty(engine, state)) {
    engine.inSync.restartProof = null
    return
  }
  if (Date.now() < proof.deadlineMs) return
  await settleRestartProof(engine, false)
}
