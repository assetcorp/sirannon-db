import { IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL, isGroupMember } from '../coordinator/group-rules.js'
import type { ReplicationEngine } from './engine.js'
import { isAnchoredToCurrentTerm } from './history-anchor.js'
import { servesItsData } from './internal-types.js'
import { positionReport } from './position-report.js'

/**
 * Sends the current primary this replica's applied position while the replica stays outside the in-sync set, so that
 * the primary, which admits replicas from their acknowledgements from feature level 2, can admit a caught-up replica
 * that receives no new writes. A repairing, faulted, or draining replica sends nothing, because it rejoins through
 * its own path.
 *
 * @param engine - The replica's engine.
 * @returns A promise that settles once the report is sent, or at once when the replica has nothing to report.
 *
 * @internal
 */
export async function reportPositionForAdmission(engine: ReplicationEngine): Promise<void> {
  const state = engine.coordinatorState
  if (!engine.running || !state || state.featureLevel < IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL) return
  const nodeId = engine.nodeId
  const primaryId = state.currentPrimary?.nodeId
  if (!primaryId || primaryId === nodeId || state.inSyncNodeIds.includes(nodeId)) return
  if (!isGroupMember(state, nodeId)) return
  if ([state.repairingNodeIds, state.faultedNodeIds, state.drainingNodeIds].some(ids => ids.includes(nodeId))) return
  if (!servesItsData(engine.syncState.phase) || !isAnchoredToCurrentTerm(engine)) return
  if (!engine.config.transport.peers().has(primaryId)) return
  const appliedSeq = await engine.log.getLastAppliedSeq(primaryId)
  await engine.config.transport.sendAck(primaryId, positionReport(engine, appliedSeq))
}
