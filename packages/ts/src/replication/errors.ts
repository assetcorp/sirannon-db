import { ReplicationError } from '../core/sync/errors.js'

export { BatchValidationError, ReplicationError } from '../core/sync/errors.js'

/** Thrown when a write conflict cannot be resolved automatically.
 * @public
 */
export class ConflictError extends ReplicationError {
  constructor(
    message: string,
    /**
     * Table the conflicting row belongs to.
     */
    public readonly table: string,
    /**
     * Primary key of the conflicting row, encoded as a string.
     */
    public readonly rowId: string,
  ) {
    super(message, 'CONFLICT_ERROR')
    this.name = 'ConflictError'
  }
}

/** Thrown when inter-node communication fails.
 * @public
 */
export class TransportError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TRANSPORT_ERROR')
    this.name = 'TransportError'
  }
}

/** Thrown when a write-concern quorum is not met within the configured timeout.
 * @public
 */
export class WriteConcernError extends ReplicationError {
  constructor(message: string) {
    super(message, 'WRITE_CONCERN_ERROR')
    this.name = 'WriteConcernError'
  }
}

/**
 * @public
 *
 * Thrown when a node cannot prove a read is as current as the caller required.
 */
export class ReadConcernError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'READ_CONCERN_ERROR', details)
    this.name = 'ReadConcernError'
  }
}

/** Thrown when a write or routing operation violates the configured topology rules.
 * @public
 */
export class TopologyError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TOPOLOGY_ERROR')
    this.name = 'TopologyError'
  }
}

/**
 * @public
 *
 * Thrown when a node cannot reach its cluster coordinator.
 */
export class CoordinatorError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'COORDINATOR_UNAVAILABLE', details)
    this.name = 'CoordinatorError'
  }
}

/**
 * @public
 *
 * Thrown when a node cannot prove it holds write authority for the current term.
 */
export class AuthorityError extends ReplicationError {
  constructor(message: string, code: string = 'AUTHORITY_LOST', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'AuthorityError'
  }
}

/**
 * @public
 *
 * Thrown when a node believing itself primary finds the group has moved to a later term.
 */
export class StalePrimaryError extends AuthorityError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'STALE_PRIMARY', details)
    this.name = 'StalePrimaryError'
  }
}

/**
 * @public
 *
 * Thrown when failover cannot complete safely.
 */
export class FailoverError extends ReplicationError {
  constructor(message: string, code: string = 'NO_SAFE_PRIMARY', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'FailoverError'
  }
}

/**
 * @public
 *
 * Thrown when no replica is in sync enough to take over as primary, so writes stay unavailable rather than risking loss.
 */
export class NoSafePrimaryError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NO_SAFE_PRIMARY', details)
    this.name = 'NoSafePrimaryError'
  }
}

/**
 * @public
 *
 * Thrown when a node cannot meet a read concern because the group does not count it as in sync.
 */
export class NodeNotInSyncError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_NOT_IN_SYNC', details)
    this.name = 'NodeNotInSyncError'
  }
}

/**
 * @public
 *
 * Thrown when a node is being taken out of service and refuses new work.
 */
export class NodeDrainingError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_DRAINING', details)
    this.name = 'NodeDrainingError'
  }
}

/**
 * @public
 *
 * Thrown when a peer speaks a replication protocol version this node cannot work with.
 */
export class ProtocolVersionMismatchError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'PROTOCOL_VERSION_MISMATCH', details)
    this.name = 'ProtocolVersionMismatchError'
  }
}

/**
 * @public
 *
 * Thrown when recovery would lose acknowledged writes, so an operator must rebuild or restore the node first.
 */
export class UnsafeRecoveryRequiredError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'UNSAFE_RECOVERY_REQUIRED', details)
    this.name = 'UnsafeRecoveryRequiredError'
  }
}

/** Thrown for initial sync failures.
 * @public
 */
export class SyncError extends ReplicationError {
  constructor(
    message: string,
    /**
     * Identifier of the sync that failed.
     */
    public readonly requestId?: string,
  ) {
    super(message, 'SYNC_ERROR')
    this.name = 'SyncError'
  }
}
