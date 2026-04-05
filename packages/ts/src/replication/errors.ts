import { SirannonError } from '../core/errors.js'

/** Base error for all replication-related failures. */
export class ReplicationError extends SirannonError {
  constructor(message: string, code: string = 'REPLICATION_ERROR') {
    super(message, code)
    this.name = 'ReplicationError'
  }
}

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

/** Thrown when an incoming replication batch fails integrity checks (checksum, schema, clock drift). */
export class BatchValidationError extends ReplicationError {
  constructor(message: string) {
    super(message, 'BATCH_VALIDATION_ERROR')
    this.name = 'BatchValidationError'
  }
}

/** Thrown when a write-concern quorum is not met within the configured timeout. */
export class WriteConcernError extends ReplicationError {
  constructor(message: string) {
    super(message, 'WRITE_CONCERN_ERROR')
    this.name = 'WriteConcernError'
  }
}

/** Thrown when a write or routing operation violates the configured topology rules. */
export class TopologyError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TOPOLOGY_ERROR')
    this.name = 'TopologyError'
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
