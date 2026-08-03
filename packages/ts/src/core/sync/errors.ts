import { SirannonError } from '../errors.js'

/** Base error for all replication-related failures.
 * @public
 */
export class ReplicationError extends SirannonError {
  constructor(
    message: string,
    code: string = 'REPLICATION_ERROR',
    /** Anything the failing operation attached, such as the peer or the batch involved. */
    public readonly details?: Record<string, unknown>,
  ) {
    super(message, code)
    this.name = 'ReplicationError'
  }
}

/** Thrown when an incoming replication batch fails integrity checks (checksum, schema, clock drift).
 * @public
 */
export class BatchValidationError extends ReplicationError {
  constructor(message: string) {
    super(message, 'BATCH_VALIDATION_ERROR')
    this.name = 'BatchValidationError'
  }
}
