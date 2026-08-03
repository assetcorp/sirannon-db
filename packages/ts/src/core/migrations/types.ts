import type { Transaction } from '../transaction.js'

/** Characters a migration name may use: letters, digits, and underscores.
 * @public
 */
export const MIGRATION_NAME_RE = /^\w+$/

/** One migration a database has already applied, as recorded in its catalogue.
 * @public
 */
export interface AppliedMigration {
  /** Version number of the migration. */
  version: number
  /** Name of the migration. */
  name: string
  /** Milliseconds since the Unix epoch, taken when the migration was applied. */
  applied_at: number
}

/**
 * Marks a migration as the point an existing database starts from, so the
 * runner records every earlier version as applied without running it.
 *
 * @public
 */
export interface MigrationBaseline {
  /** Highest version this baseline covers. */
  through: number
}

/** One schema change, with the statements that apply it and the statements that undo it.
 * @public
 */
export interface Migration {
  /** Version number. The runner applies migrations in ascending order. */
  version: number
  /** Name of the migration, using letters, digits, and underscores. */
  name: string
  /** SQL that applies the change, or a function that runs it inside the migration's transaction. */
  up: string | ((tx: Transaction) => void | Promise<void>)
  /** SQL that undoes the change, or a function that runs it. A migration without this cannot roll back. */
  down?: string | ((tx: Transaction) => void | Promise<void>)
  /** Marks this migration as the point an existing database starts from. */
  baseline?: MigrationBaseline
}

/** Migrations to apply, either as an array or as a function that produces one.
 * @public
 */
export type MigrationSource = Migration[] | (() => Migration[] | Promise<Migration[]>)

/** One migration named in a migration or rollback result.
 * @public
 */
export interface AppliedMigrationEntry {
  /** Version number of the migration. */
  version: number
  /** Name of the migration. */
  name: string
}

/** What one call to migrate did.
 * @public
 */
export interface MigrationResult {
  /** Migrations this call applied, in the order it applied them. */
  applied: AppliedMigrationEntry[]
  /** Number of migrations the database had already applied. */
  skipped: number
}

/** What one call to roll back did.
 * @public
 */
export interface RollbackResult {
  /** Migrations this call undid, newest first. */
  rolledBack: AppliedMigrationEntry[]
}
