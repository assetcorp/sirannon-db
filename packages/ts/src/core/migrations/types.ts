import type { Transaction } from '../transaction.js'

/** Matches a valid migration name, which uses only letters, digits, and underscores.
 * @public
 */
export const MIGRATION_NAME_RE = /^\w+$/

/**
 * Marks a migration as a baseline, which a new database runs in place of every
 * version up to and including `through`. A database that already has migration
 * history skips the baseline and applies only the versions that it lacks.
 *
 * @public
 */
export interface MigrationBaseline {
  /** The highest earlier version that a new database skips in favour of the baseline. */
  through: number
}

/** Describes one schema change, with the statements that apply it and the statements that undo it.
 * @public
 */
export interface Migration {
  /** The version number, which sets the ascending order in which the runner applies migrations. */
  version: number
  /** The migration's name, which uses only letters, digits, and underscores. */
  name: string
  /** The SQL that applies the change, or a function that applies it inside the migration's transaction. */
  up: string | ((tx: Transaction) => void | Promise<void>)
  /** The SQL that undoes the change, or a function that undoes it. Rollback throws a `MIGRATION_NO_DOWN` error for a migration without it. */
  down?: string | ((tx: Transaction) => void | Promise<void>)
  /** Marks this migration as a baseline, which a new database runs in place of the earlier versions. */
  baseline?: MigrationBaseline
}

/** The migrations to apply, as an array or as a function that returns one.
 * @public
 */
export type MigrationSource = Migration[] | (() => Migration[] | Promise<Migration[]>)

/** Identifies one migration in a migration or rollback result.
 * @public
 */
export interface AppliedMigrationEntry {
  /** The migration's version number. */
  version: number
  /** The migration's name. */
  name: string
}

/** Describes the outcome of one migrate call.
 * @public
 */
export interface MigrationResult {
  /** The migrations that this call applied, in the order that it applied them. */
  applied: AppliedMigrationEntry[]
  /** The number of input migrations that this call skipped, because the database already had them or a baseline replaces them. */
  skipped: number
}

/** Describes the outcome of one rollback call.
 * @public
 */
export interface RollbackResult {
  /** The migrations that this call undid, newest first. */
  rolledBack: AppliedMigrationEntry[]
}
