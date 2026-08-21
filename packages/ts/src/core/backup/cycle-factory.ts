import { BackupCycle } from './cycle.js'
import type { BackupCycleRequest } from './cycle-options.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'

/**
 * Builds the cycle that captures a database's log and checkpoints it, with a
 * deadline on every call it makes to the caller's destination. It runs nothing
 * until someone starts it.
 *
 * @param request - The operator's settings, plus the database to run against.
 * @returns The cycle.
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
