import { ReplicationError } from '../core/sync/errors.js'

export { BatchValidationError, ConflictError, ReplicationError } from '../core/sync/errors.js'

/** Sirannon throws this error when a transport call fails, such as a send to a disconnected peer or a send of a malformed message.
 * @public
 */
export class TransportError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TRANSPORT_ERROR')
    this.name = 'TransportError'
  }
}

/** Sirannon throws this error when too few peers acknowledge a write before the write-concern timeout expires, or when too few peers are connected to reach the required count.
 * @public
 */
export class WriteConcernError extends ReplicationError {
  constructor(message: string) {
    super(message, 'WRITE_CONCERN_ERROR')
    this.name = 'WriteConcernError'
  }
}

/**
 * Sirannon throws this error when a node cannot serve a read at the requested read concern, such as a `majority` read
 * on a draining or repairing node, or a read at an unsupported level.
 *
 * @public
 */
export class ReadConcernError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'READ_CONCERN_ERROR', details)
    this.name = 'ReadConcernError'
  }
}

/** Sirannon throws this error when a node that cannot accept writes receives a write that it cannot forward to a primary.
 * @public
 */
export class TopologyError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TOPOLOGY_ERROR')
    this.name = 'TopologyError'
  }
}

/**
 * Sirannon throws this error when a coordinator call fails, such as when the coordinator is unreachable, holds no state
 * for the group, or refuses an update after concurrent writes.
 *
 * @public
 */
export class CoordinatorError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'COORDINATOR_UNAVAILABLE', details)
    this.name = 'CoordinatorError'
  }
}

/**
 * Sirannon throws this error when a node cannot prove that it holds write authority for the current term, such as
 * while the node is repairing or faulted.
 *
 * @public
 */
export class AuthorityError extends ReplicationError {
  constructor(message: string, code: string = 'AUTHORITY_LOST', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'AuthorityError'
  }
}

/**
 * Sirannon throws this error when a node other than the group's current primary receives a write or another request
 * that only the primary serves, or when a replication message has an old term or the wrong group.
 *
 * @public
 */
export class StalePrimaryError extends AuthorityError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'STALE_PRIMARY', details)
    this.name = 'StalePrimaryError'
  }
}

/**
 * Reports a failover that cannot complete safely, and {@link NoSafePrimaryError} and
 * {@link UnsafeRecoveryRequiredError} extend it.
 *
 * @public
 */
export class FailoverError extends ReplicationError {
  constructor(message: string, code: string = 'NO_SAFE_PRIMARY', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'FailoverError'
  }
}

/**
 * Sirannon throws this error when no in-sync replica is eligible for promotion, or when the group has fewer than three
 * voting nodes. In that case the coordinator promotes no node, since promoting a replica outside the in-sync set could
 * lose acknowledged writes.
 *
 * @public
 */
export class NoSafePrimaryError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NO_SAFE_PRIMARY', details)
    this.name = 'NoSafePrimaryError'
  }
}

/**
 * Sirannon throws this error for a `majority` read on a node outside the group's in-sync set.
 *
 * @public
 */
export class NodeNotInSyncError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_NOT_IN_SYNC', details)
    this.name = 'NodeNotInSyncError'
  }
}

/**
 * Sirannon throws this error when a draining node receives a write.
 *
 * @public
 */
export class NodeDrainingError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_DRAINING', details)
    this.name = 'NodeDrainingError'
  }
}

/**
 * Sirannon throws this error when this node's package, specification, or protocol major version differs from a version
 * that the replication group requires.
 *
 * @public
 */
export class ProtocolVersionMismatchError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'PROTOCOL_VERSION_MISMATCH', details)
    this.name = 'ProtocolVersionMismatchError'
  }
}

/**
 * Signals that recovery would lose acknowledged writes, so an operator has to rebuild or restore the node first.
 *
 * @public
 */
export class UnsafeRecoveryRequiredError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'UNSAFE_RECOVERY_REQUIRED', details)
    this.name = 'UnsafeRecoveryRequiredError'
  }
}

/** Sirannon throws this error when a first sync fails, or when a node that is still syncing receives a read or a write.
 * @public
 */
export class SyncError extends ReplicationError {
  constructor(
    message: string,
    /**
     * Identifies the sync request that failed, when the error concerns one.
     */
    public readonly requestId?: string,
  ) {
    super(message, 'SYNC_ERROR')
    this.name = 'SyncError'
  }
}
