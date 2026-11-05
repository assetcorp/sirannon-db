import type { SQLiteConnection } from '../driver/types.js'
import { EncryptionError } from './errors.js'

/**
 * The writes that one round of a key change drained from the original, ready to apply to the new file.
 *
 * @internal
 */
export interface CapturedRound {
  readonly small: boolean
  apply(target: SQLiteConnection): Promise<void>
}

/**
 * Records the writes that commit on the original's writer while a key change runs.
 *
 * @internal
 */
export interface KeyChangeCapture {
  drain(writer: SQLiteConnection): Promise<CapturedRound>
  finish(target: SQLiteConnection): Promise<void>
  stop(): Promise<void>
}

/**
 * Returns the error for a key change that has to stop and leave the database under its current key.
 *
 * @param message - What stopped the change.
 * @param cause - The error behind it.
 * @returns The error.
 * @internal
 */
export function keyChangeAborted(message: string, cause?: unknown): EncryptionError {
  return new EncryptionError(message, 'ENCRYPTION_CHANGE_ABORTED', cause)
}

/**
 * Throws when the pool's writer is no longer the connection on which the capture started, because the capture lives
 * on that connection, and writes through any other connection go unrecorded.
 *
 * @param started - The writer on which the capture started.
 * @param current - The pool's writer now.
 * @internal
 */
export function assertSameWriter(started: SQLiteConnection, current: SQLiteConnection): void {
  if (started !== current) {
    throw keyChangeAborted(
      'The database replaced its writer connection during the key change, so writes went unrecorded',
    )
  }
}
