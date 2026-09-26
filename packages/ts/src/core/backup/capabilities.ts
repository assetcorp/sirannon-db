import type { DriverCapabilities } from '../driver/types.js'

/**
 * The backup operations that one runtime supports, so that a caller can check
 * them before it starts a backup. A runtime whose driver lacks a stepped copy
 * reports `fullCopy` as false, along with every operation that depends on it.
 *
 * @public
 */
export interface BackupCapabilities {
  /** Whether this runtime can copy an open database while writes continue. */
  fullCopy: boolean
  /** Whether this runtime can send a full copy to the destination without writing a local file. */
  streamedCopy: boolean
  /** Whether this runtime can write a full copy to a local file and then send that file to the destination. */
  stagedCopy: boolean
  /** The local disk space that a full copy needs, which is `'equal-to-backup'` when only the staged route is available. */
  localDiskRequired: 'none' | 'equal-to-backup'
  /** Whether this runtime can repeat a full copy on a schedule. */
  schedule: boolean
}

/**
 * Returns the backup operations that a runtime supports.
 *
 * @param capabilities - The capabilities that the driver declares for its runtime.
 * @param hasEngine - Whether the driver supplies a backup engine.
 * @param streams - Whether the engine can send a full copy to the destination without writing a local file.
 * @returns The backup operations that a caller can run on this runtime.
 */
export function describeBackupCapabilities(
  capabilities: DriverCapabilities,
  hasEngine: boolean,
  streams = false,
): BackupCapabilities {
  const fullCopy = hasEngine && capabilities.steppedCopy
  const streamedCopy = fullCopy && streams
  return {
    fullCopy,
    streamedCopy,
    stagedCopy: fullCopy,
    localDiskRequired: fullCopy && !streamedCopy ? 'equal-to-backup' : 'none',
    schedule: fullCopy,
  }
}
