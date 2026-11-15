import { majorityOf } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { DEFAULT_MAX_REPLICA_LAG_MS } from './constants.js'
import { hasCurrentPrimaryAuthorityFor } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { updateInSyncSetFromCurrent } from './in-sync-updates.js'

const ASSIGNED_SAMPLE_SPACING_MS = 10
const LAG_REMOVAL_PURPOSE = 'the removal of lagging replicas from the in-sync set'

/**
 * Returns how many milliseconds an in-sync replica can leave a change unacknowledged before the primary removes it.
 *
 * @param engine - The primary's engine.
 * @returns The configured `maxReplicaLagMs`, or 1000.
 *
 * @internal
 */
export function maxReplicaLagMs(engine: ReplicationEngine): number {
  return engine.config.coordinator?.maxReplicaLagMs ?? DEFAULT_MAX_REPLICA_LAG_MS
}

function sampleAssignedSeq(engine: ReplicationEngine, seq: bigint, nowMs: number): void {
  const samples = engine.inSync.assignedSamples
  const last = samples[samples.length - 1]
  if (last && (last.seq >= seq || nowMs - last.atMs < ASSIGNED_SAMPLE_SPACING_MS)) return
  samples.push({ seq, atMs: nowMs })
}

/**
 * Records the newest sequence that this node has assigned in its change log, and the time at which it assigned it,
 * so that the lag check can tell how long each replica leaves that change unacknowledged.
 *
 * @param engine - The engine of the node that wrote the change.
 * @param seq - The sequence that the write assigned.
 *
 * @internal
 */
export function noteAssignedSeq(engine: ReplicationEngine, seq: bigint): void {
  if (seq > engine.lastLocalSeq) engine.lastLocalSeq = seq
  sampleAssignedSeq(engine, seq, Date.now())
}

/**
 * Starts the lag clock afresh, so that every replica has the full lag limit to acknowledge what this node holds when
 * its term starts.
 *
 * @param engine - The primary's engine.
 *
 * @internal
 */
export function restartLagClock(engine: ReplicationEngine): void {
  engine.inSync.assignedSamples.length = 0
  engine.inSync.assignedSamples.push({ seq: engine.lastLocalSeq, atMs: Date.now() })
}

function overdueSeq(engine: ReplicationEngine, nowMs: number): bigint | null {
  const samples = engine.inSync.assignedSamples
  const cutoffMs = nowMs - maxReplicaLagMs(engine)
  while (samples.length > 1 && (samples[1]?.atMs ?? nowMs) <= cutoffMs) samples.shift()
  const oldest = samples[0]
  return oldest && oldest.atMs <= cutoffMs ? oldest.seq : null
}

function compareAcked(engine: ReplicationEngine, left: string, right: string): number {
  const leftAcked = engine.peerTracker.connectionAckedSeq(left) ?? -1n
  const rightAcked = engine.peerTracker.connectionAckedSeq(right) ?? -1n
  if (leftAcked === rightAcked) return 0
  return leftAcked < rightAcked ? -1 : 1
}

function removableLagging(state: ReplicationGroupState, lagging: string[]): string[] {
  const voters = state.votingDataBearingNodeIds
  const inSyncVoters = state.inSyncNodeIds.filter(nodeId => voters.includes(nodeId)).length
  let removableVoters = Math.max(0, inSyncVoters - majorityOf(voters.length))
  const removed: string[] = []
  for (const nodeId of lagging) {
    if (!state.inSyncNodeIds.includes(nodeId)) continue
    if (voters.includes(nodeId)) {
      if (removableVoters === 0) continue
      removableVoters -= 1
    }
    removed.push(nodeId)
  }
  return removed
}

/**
 * Removes from the in-sync set, as primary, the replicas that have not acknowledged over their current connection a
 * change that this node assigned more than `maxReplicaLagMs` earlier, most behind first. It removes only as many
 * voters as leave a majority of the voters in the set, because the coordinator refuses a removal below that floor,
 * and it sends no request when no replica can leave.
 *
 * @param engine - The primary's engine.
 * @returns A promise that settles once the coordinator has answered, or at once when no replica lags.
 *
 * @internal
 */
export async function removeLaggingReplicas(engine: ReplicationEngine): Promise<void> {
  const inSync = engine.inSync
  if (!engine.running || inSync.removing || inSync.restartProof) return
  const state = engine.coordinatorState
  if (!state || !hasCurrentPrimaryAuthorityFor(engine, state)) return
  if (engine.history.termStartTerm !== state.primaryTerm) return
  const nowMs = Date.now()
  sampleAssignedSeq(engine, engine.lastLocalSeq, nowMs)
  const overdue = overdueSeq(engine, nowMs)
  if (overdue === null) return
  const lagging = state.inSyncNodeIds
    .filter(nodeId => {
      if (nodeId === engine.nodeId) return false
      const acked = engine.peerTracker.connectionAckedSeq(nodeId)
      return acked === undefined || acked < overdue
    })
    .sort((left, right) => compareAcked(engine, left, right))
  if (removableLagging(state, lagging).length === 0) return

  inSync.removing = true
  try {
    await updateInSyncSetFromCurrent(engine, {
      purpose: LAG_REMOVAL_PURPOSE,
      known: state,
      primaryTerm: state.primaryTerm,
      change: base => {
        const removed = removableLagging(base, lagging)
        if (removed.length === 0) return null
        return { inSyncNodeIds: base.inSyncNodeIds.filter(nodeId => !removed.includes(nodeId)) }
      },
    })
  } finally {
    inSync.removing = false
  }
}
