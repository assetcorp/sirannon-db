/**
 * What one node records about a peer it is connected to.
 *
 * @public
 */
export interface NodeInfo {
  /** Identifier of the peer. */
  id: string
  /** Replication group the peer belongs to. */
  groupId?: string
  /** Whether the peer accepts writes or serves reads. */
  role: 'primary' | 'replica'
  /** Primary term the peer holds. */
  primaryTerm?: bigint
  /** Replication protocol version the peer speaks. */
  protocolVersion?: string
  /** Milliseconds since the Unix epoch, taken when the peer connected. */
  joinedAt: number
  /** Milliseconds since the Unix epoch, taken at the last message from the peer. */
  lastSeenAt: number
  /** Highest change-log position the peer has acknowledged. */
  lastAckedSeq: bigint
  /** Anything else the transport attached about the peer. */
  metadata?: Record<string, unknown>
}

/**
 * Confirms that a node applied one batch.
 *
 * @public
 */
export interface ReplicationAck {
  /** Identifier of the batch being acknowledged. */
  batchId: string
  /** Highest change-log position the sender has now applied. */
  ackedSeq: bigint
  /** Identifier of the node sending the acknowledgement. */
  nodeId: string
  /** Replication group the sender belongs to. */
  groupId?: string
  /** Primary term the sender reports as current. */
  primaryTerm?: bigint
}

/**
 * A write a replica sends to the primary, because it accepts no writes itself.
 *
 * @public
 */
export interface ForwardedTransaction {
  /** The statements to run, in order, each with its own parameters. */
  statements: Array<{ sql: string; params?: Record<string, unknown> | unknown[] }>
  /** Identifier the result echoes, so the replica matches it to the request. */
  requestId: string
  /** Replication group the forwarding replica belongs to. */
  groupId?: string
  /** Primary term the replica reports as current. */
  primaryTerm?: bigint
}

/**
 * What the primary reports back for a write a replica forwarded to it.
 *
 * @public
 */
export interface ForwardedTransactionResult {
  /** One result per statement, in the order the primary ran them. */
  results: Array<{ changes: number; lastInsertRowId: number | string }>
  /** Identifier the request carried. */
  requestId: string
  /** Replication group the primary belongs to. */
  groupId?: string
  /** Primary term the primary held when it ran the write. */
  primaryTerm?: bigint
}

/**
 * One batch a node has sent and is still waiting to see acknowledged.
 *
 * @public
 */
export interface InFlightBatch {
  /** Identifier of the batch. */
  batchId: string
  /** Change-log position of its first change. */
  fromSeq: bigint
  /** Change-log position of its last change. */
  toSeq: bigint
  /** Milliseconds since the Unix epoch, taken when the batch was sent. */
  sentAt: number
}

/**
 * Where one peer stands from this node's point of view.
 *
 * @public
 */
export interface PeerState {
  /** Identifier of the peer. */
  nodeId: string
  /** Highest change-log position the peer has acknowledged. */
  lastAckedSeq: bigint
  /** Highest change-log position this node has sent it. */
  lastSentSeq: bigint
  /** Most recent hybrid logical clock stamp received from the peer. */
  lastReceivedHlc: string
  /** Whether the transport holds an open connection to the peer. */
  connected: boolean
  /** Batches waiting to be sent to the peer. */
  pendingBatches: number
  /** Batches sent to the peer and not yet acknowledged. */
  inFlightBatches: InFlightBatch[]
}
