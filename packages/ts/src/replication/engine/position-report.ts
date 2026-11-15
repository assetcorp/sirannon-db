import type { ReplicationAck } from '../types.js'
import type { ReplicationEngine } from './engine.js'

const POSITION_REPORT_BATCH_ID = ''

/**
 * Returns a position report, which is an acknowledgement with an empty `batchId` that tells a sender the highest
 * position in its log that this node has applied, so that the sender resends every change after it.
 *
 * @param engine - The engine of the node that reports its position, which stamps its group and term on the report.
 * @param ackedSeq - The highest position in the sender's log that this node has applied.
 * @returns The stamped report.
 *
 * @internal
 */
export function positionReport(engine: ReplicationEngine, ackedSeq: bigint): ReplicationAck {
  return acknowledgement(engine, POSITION_REPORT_BATCH_ID, ackedSeq)
}

/**
 * Returns an acknowledgement stamped with this node's group and term. In coordinator mode it also names, as
 * `copyRequestId`, the last full copy that this node completed, which proves to a primary that the acknowledgement
 * describes the copied rows.
 *
 * @param engine - The engine of the node that acknowledges.
 * @param batchId - The acknowledged batch's ID, or the empty string for a position report.
 * @param ackedSeq - The highest position in the sender's log that this node has applied.
 * @returns The stamped acknowledgement.
 *
 * @internal
 */
export function acknowledgement(engine: ReplicationEngine, batchId: string, ackedSeq: bigint): ReplicationAck {
  const ack = engine.decorate({ batchId, ackedSeq, nodeId: engine.nodeId })
  const copyRequestId = engine.isCoordinatorMode() ? engine.history.copyRequestId : null
  return copyRequestId === null ? ack : { ...ack, copyRequestId }
}

/**
 * Returns true when an acknowledgement reports a node's position and acknowledges no batch.
 *
 * @param ack - The acknowledgement that a peer sent.
 * @returns True when the acknowledgement's `batchId` is empty.
 *
 * @internal
 */
export function isPositionReport(ack: ReplicationAck): boolean {
  return ack.batchId === POSITION_REPORT_BATCH_ID
}
