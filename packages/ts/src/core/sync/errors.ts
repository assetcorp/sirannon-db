import { SirannonError } from '../errors.js'

/** Reports a replication failure, and more specific replication errors such as {@link BatchValidationError} extend it.
 * @public
 */
export class ReplicationError extends SirannonError {
  constructor(
    message: string,
    code: string = 'REPLICATION_ERROR',
    /** Extra context from the code that throws the error, such as the peer or the batch involved. */
    public readonly details?: Record<string, unknown>,
  ) {
    super(message, code)
    this.name = 'ReplicationError'
  }
}

/** Sirannon throws this error when an incoming replication batch fails a validation check, such as a checksum mismatch, an invalid table name, an unsafe DDL statement, or too much clock drift.
 * @public
 */
export class BatchValidationError extends ReplicationError {
  constructor(message: string) {
    super(message, 'BATCH_VALIDATION_ERROR')
    this.name = 'BatchValidationError'
  }
}
