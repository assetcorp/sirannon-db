import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

export const DEFAULT_DESTINATION_TIMEOUT_MS = 600_000

/**
 * The longest deadline, in milliseconds, that a Node timer accepts. Node stores
 * a delay as a signed 32-bit integer and fires any longer delay after one
 * millisecond, so a longer deadline would abort every call at once.
 *
 * @internal
 */
export const LONGEST_DEADLINE_MS = 2_147_483_647

/**
 * Throws for a deadline that a timer cannot accept.
 *
 * @param timeoutMs - The deadline that the caller asks for, in milliseconds.
 * @param subject - The name of the deadline, which the error message quotes.
 * @throws A `BACKUP_ERROR` where the deadline is negative, is not a finite number, or is longer than {@link LONGEST_DEADLINE_MS}.
 *
 * @internal
 */
export function assertDeadline(timeoutMs: number, subject: string): void {
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new SirannonError(
      `${subject} must be a number of milliseconds that is zero or above, and it was ${timeoutMs}`,
      'BACKUP_ERROR',
    )
  }
  if (timeoutMs > LONGEST_DEADLINE_MS) {
    throw new SirannonError(
      `${subject} must be no longer than ${LONGEST_DEADLINE_MS}ms, which is the longest a timer holds, and it was ${timeoutMs}`,
      'BACKUP_ERROR',
    )
  }
}

/**
 * Rejects with the error from `timedOut` when an operation has not settled by
 * its deadline. Code that the caller supplies can wait forever on a socket that
 * receives no reply, and Sirannon treats that wait as work in progress, so an
 * operation without a deadline could hang indefinitely.
 *
 * @param operation - The operation to wait on.
 * @param timeoutMs - The number of milliseconds that the operation may take.
 * @param timedOut - Builds the error to reject with once that many milliseconds pass.
 * @returns The result of the operation.
 *
 * @internal
 */
export async function withinDeadline<T>(
  operation: Promise<T>,
  timeoutMs: number,
  timedOut: () => SirannonError,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | null = null
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(timedOut()), timeoutMs)
  })
  try {
    return await Promise.race([operation, deadline])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function destinationWithin<T>(operation: Promise<T>, action: string, timeoutMs: number): Promise<T> {
  return withinDeadline(
    operation,
    timeoutMs,
    () =>
      new SirannonError(
        `The destination did not ${action} within ${timeoutMs}ms, so the run stopped`,
        'BACKUP_DESTINATION_ERROR',
      ),
  )
}

/**
 * Wraps the destination of a caller so that every call to it fails with
 * `BACKUP_DESTINATION_ERROR` once it passes the deadline. A storage client can
 * leave a write or a read pending forever, and the stall deadline of the copy
 * treats that wait as work in progress, so a backup without this deadline could
 * hang indefinitely.
 *
 * @param destination - The destination that the caller supplies.
 * @param timeoutMs - The number of milliseconds that one call may take. Zero leaves the calls without a deadline.
 * @returns The destination with a deadline on every call, or the same destination where `timeoutMs` is zero.
 * @throws A `BACKUP_ERROR` where the deadline is negative, is not a finite number, or is longer than {@link LONGEST_DEADLINE_MS}.
 *
 * @internal
 */
export function destinationWithDeadline(destination: BackupDestination, timeoutMs: number): BackupDestination {
  assertDeadline(timeoutMs, 'The destination deadline')
  if (timeoutMs === 0) return destination
  const claim = destination.writePieceIfAbsent?.bind(destination)
  return {
    writePiece: (name, index, bytes) =>
      destinationWithin(destination.writePiece(name, index, bytes), `store piece ${index} of '${name}'`, timeoutMs),
    ...(claim
      ? {
          writePieceIfAbsent: (name: string, index: number, bytes: Uint8Array) =>
            destinationWithin(claim(name, index, bytes), `claim piece ${index} of '${name}'`, timeoutMs),
        }
      : {}),
    readPiece: (name, index) =>
      destinationWithin(destination.readPiece(name, index), `return piece ${index} of '${name}'`, timeoutMs),
    listPieces: name => destinationWithin(destination.listPieces(name), `list the pieces of '${name}'`, timeoutMs),
  }
}
