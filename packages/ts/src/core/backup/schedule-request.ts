import type { BackupScheduleOptions } from '../types.js'

/** Runs an operation while no other operation holds the writer, so that the copy has the writer connection to itself.
 * @public
 */
export type RunExclusive = (op: () => Promise<void>) => Promise<void>

/**
 * The options of one repeating backup, plus the database details that Sirannon
 * puts in every report of that backup.
 *
 * @public
 */
export interface BackupScheduleRequest extends BackupScheduleOptions {
  /** The identifier of the database that Sirannon copies. Defaults to the name of the source file without its extension. */
  databaseId?: string
  /** The file that Sirannon copies. Defaults to the file that SQLite has open on the connection. */
  sourcePath?: string
  /** Runs each copy while no other operation holds the writer. By default, Sirannon runs each copy without a lock. */
  runExclusive?: RunExclusive
}
