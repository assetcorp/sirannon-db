import type { DatabaseCopyStep } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import {
  deserializeError,
  type SerializedError,
  WORKER_CANCELLED_CODE,
  WORKER_COPY_STOPPED_CODE,
  type WorkerRequestBody,
} from './protocol.js'

/**
 * Holds what the host keeps for one request that it has sent to the writer worker and that has no reply yet: the
 * callbacks that settle the caller's promise, the timers that bound the wait, and the request's kind.
 *
 * @internal
 */
export interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: NodeJS.Timeout | null
  graceTimer: NodeJS.Timeout | null
  deadlineMs: number
  cancellable: boolean
  kind: WorkerRequestBody['kind']
  onStep?: (step: DatabaseCopyStep) => void
}

/**
 * Returns the error that the host gives a caller when the writer worker sends no reply within the deadline.
 *
 * @param waitedMs - How long the host waited, in milliseconds.
 * @param kind - The request's kind, which selects the error.
 * @returns An error with code `BACKUP_STALLED` for a database copy, and with code `WRITER_WORKER_TIMEOUT` for every
 * other request.
 * @internal
 */
export function unresponsiveError(waitedMs: number, kind?: WorkerRequestBody['kind']): SirannonError {
  if (kind === 'copyDatabase') {
    return new SirannonError(
      `The writer worker moved no page of the copy for ${waitedMs}ms, so the copy was stopped and nothing it wrote is usable`,
      'BACKUP_STALLED',
    )
  }
  return new SirannonError(
    `Writer worker did not respond within ${waitedMs}ms; the operation's outcome is unknown`,
    'WRITER_WORKER_TIMEOUT',
  )
}

/**
 * Returns the error that the host gives a caller whose request the writer worker answered with an error.
 *
 * @param entry - The request.
 * @param error - The error that the worker sent.
 * @returns `WRITE_OVERLOADED` for a request that the worker dropped before it began, the unresponsive error for a
 * stopped copy, and the worker's own error otherwise.
 * @internal
 */
export function failedReplyError(entry: PendingRequest, error: SerializedError): Error {
  if (error.code === WORKER_CANCELLED_CODE) {
    return new SirannonError(
      `The writer worker could not take this operation within ${entry.deadlineMs}ms; it was not applied and is safe to retry`,
      'WRITE_OVERLOADED',
    )
  }
  if (error.code === WORKER_COPY_STOPPED_CODE) return unresponsiveError(entry.deadlineMs, entry.kind)
  return deserializeError(error)
}

/**
 * Returns how long the host waits for the next page of a database copy before it gives the copy up.
 *
 * @param request - The request.
 * @returns The wait in milliseconds for a database copy, or `undefined` for every other request.
 * @internal
 */
export function copyStallDeadline(request: WorkerRequestBody): number | undefined {
  return request.kind === 'copyDatabase' ? request.stallTimeoutMs : undefined
}

/**
 * Cancels both timers of a request that has no reply yet.
 *
 * @param entry - The request.
 * @internal
 */
export function clearPendingTimers(entry: PendingRequest): void {
  if (entry.timer) clearTimeout(entry.timer)
  if (entry.graceTimer) clearTimeout(entry.graceTimer)
}
