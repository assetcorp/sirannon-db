import { MigrationError } from '../errors.js'
import { SQLITE_USER_VERSION_MAX } from './baseline.js'
import { runsWithoutForeignKeys } from './foreign-keys.js'
import { MIGRATION_NAME_RE, type Migration } from './types.js'

function isMigrationStep(step: unknown): boolean {
  return typeof step === 'string' || typeof step === 'function'
}

/**
 * Returns the migrations in ascending version order once each one passes validation.
 *
 * @param migrations - The migrations that a caller passed.
 * @returns A sorted copy of `migrations`.
 * @throws {@link MigrationError} with `MIGRATION_VALIDATION_ERROR` for a malformed migration, and with
 * `MIGRATION_DUPLICATE_VERSION` when two migrations share a version.
 * @internal
 */
export function validateMigrations(migrations: Migration[]): Migration[] {
  if (!Array.isArray(migrations)) {
    throw new MigrationError('Migrations must be an array of migrations', 0, 'MIGRATION_VALIDATION_ERROR')
  }

  const seenVersions = new Map<number, string>()

  for (const m of migrations) {
    if (typeof m !== 'object' || m === null) {
      throw new MigrationError(
        `Each migration must be an object with a version, a name, and an up; received ${m === null ? 'null' : typeof m}`,
        0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (!Number.isSafeInteger(m.version) || m.version <= 0 || m.version > SQLITE_USER_VERSION_MAX) {
      throw new MigrationError(
        `Invalid migration version: ${m.version}. Versions must be integers from 1 to ${SQLITE_USER_VERSION_MAX} so they mirror to PRAGMA user_version`,
        typeof m.version === 'number' && Number.isFinite(m.version) ? m.version : 0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (typeof m.name !== 'string' || !MIGRATION_NAME_RE.test(m.name)) {
      throw new MigrationError(`Invalid migration name: '${String(m.name)}'`, m.version, 'MIGRATION_VALIDATION_ERROR')
    }

    const existing = seenVersions.get(m.version)
    if (existing) {
      throw new MigrationError(
        `Duplicate migration version ${m.version}: '${existing}' and '${m.name}'`,
        m.version,
        'MIGRATION_DUPLICATE_VERSION',
      )
    }
    seenVersions.set(m.version, m.name)

    if (!isMigrationStep(m.up)) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} must have an up that is SQL text or a function`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (typeof m.up === 'string' && m.up.trim().length === 0) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} has empty up SQL`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (m.down !== undefined && !isMigrationStep(m.down)) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} must leave down unset or set it to SQL text or a function`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (typeof m.down === 'string' && m.down.trim().length === 0) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} has empty down SQL`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (m.withoutForeignKeys !== undefined && typeof m.withoutForeignKeys !== 'boolean') {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} must leave withoutForeignKeys unset or set it to true or false`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (
      m.withoutForeignKeys === true &&
      typeof m.up === 'string' &&
      !runsWithoutForeignKeys({ ...m, withoutForeignKeys: false }, m.up)
    ) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} must put PRAGMA foreign_keys = OFF in its up SQL when withoutForeignKeys is true, because a device receives only the SQL`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (m.baseline !== undefined && (typeof m.baseline !== 'object' || m.baseline === null)) {
      throw new MigrationError(
        `Migration ${m.version}_${m.name} must set baseline to an object with a through version`,
        m.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }
  }

  return [...migrations].sort((a, b) => a.version - b.version)
}
