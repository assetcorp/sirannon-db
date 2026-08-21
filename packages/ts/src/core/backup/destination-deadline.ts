import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

export const DEFAULT_DESTINATION_TIMEOUT_MS = 600_000

async function withinDeadline<T>(operation: Promise<T>, action: string, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | null = null
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new SirannonError(
          `The destination did not ${action} within ${timeoutMs}ms, so the run stopped`,
          'BACKUP_DESTINATION_ERROR',
        ),
      )
    }, timeoutMs)
  })
  try {
    return await Promise.race([operation, deadline])
  } finally {
    if (timer) clearTimeout(timer)
  }
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
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new SirannonError(
      `The destination deadline must be a number of milliseconds that is zero or above, and it was ${timeoutMs}`,
      'BACKUP_ERROR',
    )
  }
  if (timeoutMs === 0) return destination
  const claim = destination.writePieceIfAbsent?.bind(destination)
  return {
    writePiece: (name, index, bytes) =>
      withinDeadline(destination.writePiece(name, index, bytes), `store piece ${index} of '${name}'`, timeoutMs),
    ...(claim
      ? {
          writePieceIfAbsent: (name: string, index: number, bytes: Uint8Array) =>
            withinDeadline(claim(name, index, bytes), `claim piece ${index} of '${name}'`, timeoutMs),
        }
      : {}),
    readPiece: (name, index) =>
      withinDeadline(destination.readPiece(name, index), `return piece ${index} of '${name}'`, timeoutMs),
    listPieces: name => withinDeadline(destination.listPieces(name), `list the pieces of '${name}'`, timeoutMs),
  }
}
