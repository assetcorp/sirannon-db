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
  return engine.decorate({ batchId: POSITION_REPORT_BATCH_ID, ackedSeq, nodeId: engine.nodeId })
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
