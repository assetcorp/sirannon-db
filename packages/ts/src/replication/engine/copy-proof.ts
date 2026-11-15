import type { ReplicationAck, SyncRequest } from '../types.js'
import type { ReplicationEngine } from './engine.js'

/**
 * Forgets, as primary, what a joiner acknowledged over its current connection, because the copy that the joiner
 * requests replaces the rows that those acknowledgements describe. For a request that names no history transaction,
 * which this node answers with a full copy, it also records the request, so that from then on only an acknowledgement
 * that names this copy admits the joiner or counts toward a write concern.
 *
 * @param engine - The primary's engine.
 * @param joinerNodeId - The joiner's node ID.
 * @param request - The joiner's sync request.
 *
 * @internal
 */
export function resetJoinerForCopy(engine: ReplicationEngine, joinerNodeId: string, request: SyncRequest): void {
  engine.peerTracker.forgetConnectionAcks(joinerNodeId)
  const term = engine.coordinatorState?.primaryTerm
  if (request.historyTail || term === undefined) return
  engine.inSync.copyProofs.set(joinerNodeId, { requestId: request.requestId, term })
}

/**
 * Returns whether an acknowledgement may admit its sender to the in-sync set and count toward a write concern. After
 * this node resets a joiner for a full copy, only an acknowledgement whose `copyRequestId` names that copy qualifies,
 * until this node resets the joiner for another copy or the group moves to a later term. Every other acknowledgement
 * qualifies, and this node uses any acknowledgement to choose the next batch that it sends.
 *
 * @param engine - The primary's engine.
 * @param ack - The acknowledgement, checked once this node has validated its group and term.
 * @returns True when the acknowledgement counts.
 *
 * @internal
 */
export function creditsAcknowledgement(engine: ReplicationEngine, ack: ReplicationAck): boolean {
  const proofs = engine.inSync.copyProofs
  const required = proofs.get(ack.nodeId)
  if (required === undefined) return true
  const term = engine.coordinatorState?.primaryTerm
  if (term === undefined) return false
  if (term > required.term) {
    proofs.delete(ack.nodeId)
    return true
  }
  return term === required.term && ack.copyRequestId === required.requestId
}
