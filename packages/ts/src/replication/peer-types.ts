/**
 * Describes a connected peer, as the transport records it.
 *
 * @public
 */
export interface NodeInfo {
  /** Identifies the peer. */
  id: string
  /** Identifies the peer's replication group. */
  groupId?: string
  /** Records whether the peer is the primary or a replica. */
  role: 'primary' | 'replica'
  /** Holds the primary term that the peer reports. */
  primaryTerm?: bigint
  /** Holds the replication protocol version that the peer uses. */
  protocolVersion?: string
  /** Holds the time, in milliseconds since the Unix epoch, at which the peer connected. */
  joinedAt: number
  /** Holds the time, in milliseconds since the Unix epoch, of the last message from the peer. The bundled transports set it only when the peer connects. */
  lastSeenAt: number
  /** Holds the highest change-log position that the peer acknowledges. The bundled transports set it to `0n` only when the peer connects, while {@link PeerState.lastAckedSeq} follows each acknowledgement. */
  lastAckedSeq: bigint
  /** Holds any other data that the transport attaches about the peer. */
  metadata?: Record<string, unknown>
}

/**
 * Confirms that a node applied one batch.
 *
 * @public
 */
export interface ReplicationAck {
  /** Identifies the batch that the node acknowledges. */
  batchId: string
  /** Holds the highest change-log position that the sender applied. */
  ackedSeq: bigint
  /** Identifies the node that sends the acknowledgement. */
  nodeId: string
  /** Identifies the sender's replication group. */
  groupId?: string
  /** Holds the primary term that the sender reports as current. */
  primaryTerm?: bigint
}

/**
 * Describes a write that a replica sends to the primary, because the replica cannot accept writes itself.
 *
 * @public
 */
export interface ForwardedTransaction {
  /** Lists the statements to execute, in order, each with its own parameters. */
  statements: Array<{ sql: string; params?: Record<string, unknown> | unknown[] }>
  /** Identifies this forwarded request. */
  requestId: string
  /** Identifies the forwarding replica's replication group. */
  groupId?: string
  /** Holds the primary term that the replica reports as current. */
  primaryTerm?: bigint
}

/**
 * Describes the result that the primary returns for a forwarded write.
 *
 * @public
 */
export interface ForwardedTransactionResult {
  /** Holds one result per statement, in the order that the primary executed them. */
  results: Array<{ changes: number; lastInsertRowId: number | string }>
  /** Holds the request ID that the primary generates for this execution, which differs from the ID in the forwarded request. */
  requestId: string
  /** Identifies the primary's replication group. */
  groupId?: string
  /** Holds the term under which the primary executed the write. */
  primaryTerm?: bigint
}

/**
 * Describes one batch that this node sent and that its peer has yet to acknowledge.
 *
 * @public
 */
export interface InFlightBatch {
  /** Identifies the batch. */
  batchId: string
  /** Holds the change-log position of the batch's first change. */
  fromSeq: bigint
  /** Holds the change-log position of the batch's last change. */
  toSeq: bigint
  /** Holds the time, in milliseconds since the Unix epoch, at which this node sent the batch. */
  sentAt: number
}

/**
 * Describes one peer's replication progress, as this node tracks it.
 *
 * @public
 */
export interface PeerState {
  /** Identifies the peer. */
  nodeId: string
  /** Holds the highest change-log position among the peer's acknowledgements. */
  lastAckedSeq: bigint
  /** Holds the change-log position up to which this node sends batches to the peer. A timeout or a failed send moves it back. */
  lastSentSeq: bigint
  /** Holds the most recent hybrid logical clock stamp from the peer. The engine sets it to an empty string when it adds the peer, and never updates it. */
  lastReceivedHlc: string
  /** Is true while the transport has an open connection to the peer. */
  connected: boolean
  /** Counts the batches that this node sent to the peer and that await acknowledgement. */
  pendingBatches: number
  /** Lists the batches that this node sent to the peer and that the peer has yet to acknowledge. */
  inFlightBatches: InFlightBatch[]
}
