import { ReplicationError } from '../core/sync/errors.js'

export { BatchValidationError, ReplicationError } from '../core/sync/errors.js'

/** Thrown when a write conflict cannot be resolved automatically. */
export class ConflictError extends ReplicationError {
  constructor(
    message: string,
    public readonly table: string,
    public readonly rowId: string,
  ) {
    super(message, 'CONFLICT_ERROR')
    this.name = 'ConflictError'
  }
}

/** Thrown when inter-node communication fails. */
export class TransportError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TRANSPORT_ERROR')
    this.name = 'TransportError'
  }
}

/** Thrown when a write-concern quorum is not met within the configured timeout. */
export class WriteConcernError extends ReplicationError {
  constructor(message: string) {
    super(message, 'WRITE_CONCERN_ERROR')
    this.name = 'WriteConcernError'
  }
}

export class ReadConcernError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'READ_CONCERN_ERROR', details)
    this.name = 'ReadConcernError'
  }
}

/** Thrown when a write or routing operation violates the configured topology rules. */
export class TopologyError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TOPOLOGY_ERROR')
    this.name = 'TopologyError'
  }
}

export class CoordinatorError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'COORDINATOR_UNAVAILABLE', details)
    this.name = 'CoordinatorError'
  }
}

export class AuthorityError extends ReplicationError {
  constructor(message: string, code: string = 'AUTHORITY_LOST', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'AuthorityError'
  }
}

export class StalePrimaryError extends AuthorityError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'STALE_PRIMARY', details)
    this.name = 'StalePrimaryError'
  }
}

export class FailoverError extends ReplicationError {
  constructor(message: string, code: string = 'NO_SAFE_PRIMARY', details?: Record<string, unknown>) {
    super(message, code, details)
    this.name = 'FailoverError'
  }
}

export class NoSafePrimaryError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NO_SAFE_PRIMARY', details)
    this.name = 'NoSafePrimaryError'
  }
}

export class NodeNotInSyncError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_NOT_IN_SYNC', details)
    this.name = 'NodeNotInSyncError'
  }
}

export class NodeDrainingError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NODE_DRAINING', details)
    this.name = 'NodeDrainingError'
  }
}

export class ProtocolVersionMismatchError extends ReplicationError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'PROTOCOL_VERSION_MISMATCH', details)
    this.name = 'ProtocolVersionMismatchError'
  }
}

export class UnsafeRecoveryRequiredError extends FailoverError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'UNSAFE_RECOVERY_REQUIRED', details)
    this.name = 'UnsafeRecoveryRequiredError'
  }
}

/** Thrown for initial sync failures. */
export class SyncError extends ReplicationError {
  constructor(
    message: string,
    public readonly requestId?: string,
  ) {
    super(message, 'SYNC_ERROR')
    this.name = 'SyncError'
  }
}
