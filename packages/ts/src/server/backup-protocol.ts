import type { BackupChain, BackupChainRecord } from '../core/backup/chain.js'
import type { BackupCycleStatus } from '../core/backup/cycle-status.js'
import { MAX_RESTORE_BATCH_SIZE } from '../core/backup/restore-options.js'
import type { BackupVerifyResult } from '../core/backup/verify.js'

export const BACKUP_RESTORE_NOT_ACCEPTED_MESSAGE =
  'This server does not restore databases over the wire; turn on acceptBackupRestore to open the route'

/** The answer to a backup a caller triggered, which the server takes on and does not wait for.
 * @public
 */
export interface BackupTriggerResponse {
  /** True whenever the server answers, since it answers only once it has accepted the turn. */
  started: true
}

/** What the cycle is doing, and what its recent turns produced.
 * @public
 */
export type BackupStatusResponse = BackupCycleStatus

/** Every chain the destination stores.
 * @public
 */
export interface BackupChainResponse {
  /** One entry per chain, newest first, each with its full copy and its change pieces in order. */
  chains: BackupChain[]
}

/** Names the backup to read back out of the destination.
 * @public
 */
export interface BackupVerifyRequest {
  /** Name the backup is stored under, which every chain record states. */
  name?: unknown
}

/** What Sirannon found when it read one backup back.
 * @public
 */
export type BackupVerifyResponse = BackupVerifyResult

/** How far back a restore must still reach.
 * @public
 */
export interface BackupSafeToDeleteRequest {
  /** Epoch milliseconds of the earliest moment a restore must still reach. Leave it out and the answer covers only the backups no restore could ever use. */
  restorableFrom?: unknown
}

/** The records no restore still needs.
 * @public
 */
export interface BackupSafeToDeleteResponse {
  /** The records you may delete, oldest first. */
  records: BackupChainRecord[]
}

/** The moment to rebuild the database at, and how many change pieces one batch replays.
 * @public
 */
export interface BackupRestoreRequest {
  /** Epoch milliseconds you want back. Leave it out for the newest backup the destination stores. */
  moment?: unknown
  /** How many change pieces to replay between one checkpoint and the next. Leave it out for the default of 16. */
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
 * Refuses a verify request that names no backup.
 *
 * @param value - What the request supplied under `name`.
 * @returns The refusal, or null where the request names one.
 */
export function verifyNameValidationError(value: unknown): string | null {
  if (typeof value !== 'string' || value.length === 0) {
    return 'Field "name" is required and must be the name a chain record states'
  }
  return null
}

/**
 * Refuses a safe-to-delete request whose earliest moment is not a moment.
 *
 * @param value - What the request supplied under `restorableFrom`.
 * @returns The refusal, or null where the request supplied a moment or nothing at all.
 */
export function restorableFromValidationError(value: unknown): string | null {
  return wholeNumberError('restorableFrom', value, 0)
}

/**
 * Refuses a restore request whose moment or batch size Sirannon could not use.
 *
 * @param body - What the request supplied.
 * @returns The refusal, or null where both fields are usable or absent.
 */
export function restoreRequestValidationError(body: BackupRestoreRequest): string | null {
  return (
    wholeNumberError('moment', body.moment, 0) ??
    wholeNumberError('batchSize', body.batchSize, 1, MAX_RESTORE_BATCH_SIZE)
  )
}
