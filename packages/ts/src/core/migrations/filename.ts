import { MigrationError } from '../errors.js'

/**
 * Matches a migration file name of the form `001_create_orders.up.sql`.
 *
 * @public
 */
export const MIGRATION_FILENAME_PATTERN = /^(\d+)_(\w+)\.(up|down)\.sql$/

/**
 * The three parts of a migration file name.
 *
 * @public
 */
export interface ParsedMigrationFilename {
  /**
   * Version number the file name starts with.
   */
  version: number
  /**
   * Migration name between the version and the direction.
   */
  name: string
  /**
   * Whether the file applies the migration or undoes it.
   */
  direction: 'up' | 'down'
}

/**
 * Splits a migration file name into its version, name, and direction.
 *
 * @param filename - File name such as `001_create_orders.up.sql`.
 * @returns The three parts, or null when the name does not match the expected form.
 *
 * @public
 */
export function parseMigrationFilename(filename: string): ParsedMigrationFilename | null {
  const match = MIGRATION_FILENAME_PATTERN.exec(filename)
  if (!match) return null

  const version = parseInt(match[1], 10)
  if (!Number.isFinite(version) || version <= 0 || !Number.isSafeInteger(version)) {
    throw new MigrationError(`Invalid migration version: ${match[1]}`, version, 'MIGRATION_VALIDATION_ERROR')
  }

  return { version, name: match[2], direction: match[3] as 'up' | 'down' }
}
