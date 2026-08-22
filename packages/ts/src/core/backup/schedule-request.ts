import type { BackupScheduleOptions } from '../types.js'

/** Runs a backup while nothing else holds the writer, so that the copy never shares the writer connection with another write.
 * @public
 */
export type RunExclusive = (op: () => Promise<void>) => Promise<void>

/**
 * What one repeating backup needs beyond the caller's own options, so that
 * every report it produces names the database the copy came from.
 *
 * @public
 */
export interface BackupScheduleRequest extends BackupScheduleOptions {
  /** Database the copies are taken from. Defaults to the name of the file SQLite has open. */
  databaseId?: string
  /** File the copies are taken from. Defaults to the file SQLite has open on the connection. */
  sourcePath?: string
  /** Runs each copy with nothing else holding the writer. Defaults to running it with no lock of its own. */
  runExclusive?: RunExclusive
}
