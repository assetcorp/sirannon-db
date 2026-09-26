import type { BackupChain, BackupChainRecord } from '../core/backup/chain.js'
import type { BackupCycleStatus } from '../core/backup/cycle-status.js'
import { MAX_RESTORE_BATCH_SIZE } from '../core/backup/restore-options.js'
import type { BackupVerifyResult } from '../core/backup/verify.js'

export const BACKUP_RESTORE_NOT_ACCEPTED_MESSAGE =
  'This server does not restore databases over the wire; turn on acceptBackupRestore to open the route'

/** The server's answer when a caller triggers a backup, which it sends once it accepts the turn and before the backup finishes.
 * @public
 */
export interface BackupTriggerResponse {
  /** Always true, because the server sends this answer only after it accepts the turn. */
  started: true
}

/** The checkpoint cycle's current activity, and the results of its recent turns.
 * @public
 */
export type BackupStatusResponse = BackupCycleStatus

/** Every chain in the backup destination.
 * @public
 */
export interface BackupChainResponse {
  /** One entry per chain, newest first, each with its full copy and its change pieces in order. */
  chains: BackupChain[]
}

/** The backup to read back from the destination.
 * @public
 */
export interface BackupVerifyRequest {
  /** The name under which the destination stores the backup, as it appears in every chain record. */
  name?: unknown
}

/** The result of reading one backup back from the destination.
 * @public
 */
export type BackupVerifyResponse = BackupVerifyResult

/** How far back you must still be able to restore.
 * @public
 */
export interface BackupSafeToDeleteRequest {
  /** The earliest moment, in epoch milliseconds, that you must still be able to restore to. When it is absent, the server lists only the backups that you cannot restore from. */
  restorableFrom?: unknown
}

/** The records that you no longer need for any restore.
 * @public
 */
export interface BackupSafeToDeleteResponse {
  /** The records that you can delete, oldest first. */
  records: BackupChainRecord[]
}

/** The moment to which the server rebuilds the database, and how many change pieces it replays in one batch.
 * @public
 */
export interface BackupRestoreRequest {
  /** The moment, in epoch milliseconds, to restore the database to. When it is absent, the server uses the current time, so it restores the newest state that the destination stores. */
  moment?: unknown
  /** How many change pieces the server replays between one checkpoint and the next. Defaults to 16. */
  batchSize?: unknown
}

function wholeNumberError(field: string, value: unknown, atLeast: number, atMost?: number): string | null {
  if (value === undefined) return null
  if (typeof value !== 'number' || !Number.isInteger(value) || value < atLeast) {
    return `Field "${field}" must be a whole number of at least ${atLeast}`
  }
  if (atMost !== undefined && value > atMost) {
    return `Field "${field}" must be a whole number of at most ${atMost}`
  }
  return null
}

/**
 * Returns an error message when a verify request names no backup.
 *
 * @param value - The value that the request sends under `name`.
 * @returns The error message, or null when the request names a backup.
 */
export function verifyNameValidationError(value: unknown): string | null {
  if (typeof value !== 'string' || value.length === 0) {
    return 'Field "name" is required and must be the name a chain record states'
  }
  return null
}

/**
 * Returns an error message when the `restorableFrom` of a safe-to-delete request is not a whole number of at least 0.
 *
 * @param value - The value that the request sends under `restorableFrom`.
 * @returns The error message, or null when the value is a valid moment or absent.
 */
export function restorableFromValidationError(value: unknown): string | null {
  return wholeNumberError('restorableFrom', value, 0)
}

/**
 * Returns an error message when the moment or the batch size of a restore request is invalid.
 *
 * @param body - The body of the restore request.
 * @returns The error message, or null when both fields are valid or absent.
 */
export function restoreRequestValidationError(body: BackupRestoreRequest): string | null {
  return (
    wholeNumberError('moment', body.moment, 0) ??
    wholeNumberError('batchSize', body.batchSize, 1, MAX_RESTORE_BATCH_SIZE)
  )
}
