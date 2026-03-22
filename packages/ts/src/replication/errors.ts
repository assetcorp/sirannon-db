import { SirannonError } from '../core/errors.js'

export class ReplicationError extends SirannonError {
  constructor(message: string, code: string = 'REPLICATION_ERROR') {
    super(message, code)
    this.name = 'ReplicationError'
  }
}

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

export class TransportError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TRANSPORT_ERROR')
    this.name = 'TransportError'
  }
}

export class BatchValidationError extends ReplicationError {
  constructor(message: string) {
    super(message, 'BATCH_VALIDATION_ERROR')
    this.name = 'BatchValidationError'
  }
}

export class WriteConcernError extends ReplicationError {
  constructor(message: string) {
    super(message, 'WRITE_CONCERN_ERROR')
    this.name = 'WriteConcernError'
  }
}

export class TopologyError extends ReplicationError {
  constructor(message: string) {
    super(message, 'TOPOLOGY_ERROR')
    this.name = 'TopologyError'
  }
}

export class RaftError extends ReplicationError {
  constructor(message: string) {
    super(message, 'RAFT_ERROR')
    this.name = 'RaftError'
  }
}
