import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

export const DEFAULT_DESTINATION_TIMEOUT_MS = 600_000

/**
 * The longest deadline a timer can hold. Node counts a delay in a signed
 * 32-bit integer and fires anything above this straight away, so a deadline
 * past it would abort every call at once.
 *
 * @internal
 */
export const LONGEST_DEADLINE_MS = 2_147_483_647

/**
 * Refuses a deadline no timer could hold.
 *
 * @param timeoutMs - Milliseconds the caller asked for.
 * @param subject - What the deadline is called, which the error names.
 * @throws A `BACKUP_ERROR` where the deadline is negative, is not a number, or is longer than a timer can hold.
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
 * Fails an operation that has not settled by its deadline. A caller's own code
 * can wait on a socket that never answers, and Sirannon counts that wait as
 * work in progress, so an operation without a deadline would never end.
 *
 * @param operation - What to wait on.
 * @param timeoutMs - Milliseconds it may take.
 * @param timedOut - Builds the error to fail with once that many milliseconds pass.
 * @returns What the operation produced.
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
 * Wraps a caller's destination so every call to it fails once it passes the
 * deadline. A storage client can leave a write or a read pending forever, and
 * the copy's own stall deadline counts that wait as work in progress. A run
 * without this deadline would therefore never end.
 *
 * @param destination - Destination the caller supplied.
 * @param timeoutMs - Milliseconds one call may take. Zero leaves the calls unbounded.
 * @returns The same destination with a deadline on every call it answers.
 * @throws A `BACKUP_ERROR` where the deadline is negative or is not a number.
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
