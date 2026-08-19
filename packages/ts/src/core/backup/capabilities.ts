import type { DriverCapabilities } from '../driver/types.js'

/**
 * What one runtime supports of the backup operations, so a caller learns
 * before a run rather than at failure time. A runtime that hands over whole
 * databases only reports no full copy at all.
 *
 * @public
 */
export interface BackupCapabilities {
  /** Whether this runtime copies an open database while writes continue. */
  fullCopy: boolean
  /** Whether a full copy reaches the destination without a local file. */
  streamedCopy: boolean
  /** Whether a full copy writes a local file and sends that file on. */
  stagedCopy: boolean
  /** Local disk a full copy needs, which the staged route sets to the size of the backup. */
  localDiskRequired: 'none' | 'equal-to-backup'
  /** Whether this runtime repeats a full copy on a schedule. */
  schedule: boolean
}

/**
 * Reports which backup operations a runtime supports.
 *
 * @param capabilities - What the driver declares its runtime supports.
 * @param hasEngine - Whether the driver supplies a backup engine.
 * @param streams - Whether the engine carries a full copy to the destination without a local file.
 * @returns The backup operations a caller can run on this runtime.
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
