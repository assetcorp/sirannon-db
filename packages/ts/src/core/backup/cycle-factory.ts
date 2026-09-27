import { BackupCycle } from './cycle.js'
import type { BackupCycleRequest } from './cycle-options.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'

/**
 * Returns a backup cycle that captures the write-ahead log of a database and
 * then checkpoints it, with a deadline on every call to the destination. The
 * cycle starts work only when the caller starts it.
 *
 * @param request - The settings of the operator, and the database that the cycle backs up.
 * @returns The backup cycle, which the caller has yet to start.
 */
export function createBackupCycle(request: BackupCycleRequest): BackupCycle {
  return new BackupCycle({
    ...request,
    destination: destinationWithDeadline(
      request.destination,
      request.destinationTimeoutMs ?? DEFAULT_DESTINATION_TIMEOUT_MS,
    ),
  })
}
