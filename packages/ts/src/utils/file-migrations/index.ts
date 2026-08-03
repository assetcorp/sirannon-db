import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { MigrationError } from '../../core/errors.js'
import { applyBaselineOption, type BaselineFileOption } from '../../core/migrations/baseline.js'
import { parseMigrationFilename } from '../../core/migrations/filename.js'
import { LAZY_DOWN_SQL } from '../../core/migrations/lazy-down.js'
import type { Migration } from '../../core/migrations/types.js'

/**
 * @public
 *
 * How a directory of migration files is turned into migrations.
 */
export interface LoadMigrationsOptions {
  /**
   * Marks the migration an existing database starts from.
   */
  baseline?: BaselineFileOption
}

/**
 * @public
 *
 * One migration found on disk, with the paths of its up and down files.
 */
export interface ScannedMigration {
  /**
   * Version number the file name starts with.
   */
  version: number
  /**
   * Migration name between the version and the direction.
   */
  name: string
  /**
   * Path of the file that applies the migration.
   */
  upPath: string
  /**
   * Path of the file that undoes it, or null when the migration has none.
   */
  downPath: string | null
}

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    if (s.charCodeAt(i) <= 0x1f) return true
  }
  return false
}

/**
 * @public
 *
 * Lists the migration files in a directory, in ascending version order.
 *
 * @param dirPath - Directory holding the migration files.
 * @returns One entry per migration, with the paths of its up and down files.
 * @throws When the path is unsafe or a file name does not parse.
 */
export function scanDirectory(dirPath: string): ScannedMigration[] {
  if (hasControlCharacters(dirPath)) {
    throw new MigrationError('Migration path contains invalid characters', 0, 'MIGRATION_VALIDATION_ERROR')
  }

  const segments = dirPath.split(/[/\\]/)
  if (segments.includes('..')) {
    throw new MigrationError(
      'Migration path must not contain directory traversal segments',
      0,
      'MIGRATION_VALIDATION_ERROR',
    )
  }

  const resolvedPath = resolve(dirPath)

  let stat: ReturnType<typeof statSync> | undefined
  try {
    stat = statSync(resolvedPath)
  } catch {
    throw new MigrationError(`Migrations path does not exist: ${resolvedPath}`, 0)
  }

  if (!stat.isDirectory()) {
    throw new MigrationError(`Migrations path is not a directory: ${resolvedPath}`, 0)
  }

  const entries = readdirSync(resolvedPath, { withFileTypes: true })
  const grouped = new Map<number, { name: string; upPath?: string; downPath?: string }>()

  for (const entry of entries) {
    if (!entry.isFile()) continue

    const parsed = parseMigrationFilename(entry.name)
    if (!parsed) continue

    const { version, name, direction } = parsed

    const existing = grouped.get(version)
    if (existing && existing.name !== name) {
      throw new MigrationError(
        `Duplicate migration version ${version}: '${existing.name}' and '${name}'`,
        version,
        'MIGRATION_DUPLICATE_VERSION',
      )
    }

    const filePath = join(resolvedPath, entry.name)
    if (!existing) {
      grouped.set(version, {
        name,
        [direction === 'up' ? 'upPath' : 'downPath']: filePath,
      })
    } else {
      if (direction === 'up') existing.upPath = filePath
      else existing.downPath = filePath
    }
  }

  const results: ScannedMigration[] = []
  for (const [version, entry] of grouped) {
    if (!entry.upPath) {
      throw new MigrationError(
        `Migration version ${version} (${entry.name}) is missing an .up.sql file`,
        version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }
    results.push({
      version,
      name: entry.name,
      upPath: entry.upPath,
      downPath: entry.downPath ?? null,
    })
  }

  results.sort((a, b) => a.version - b.version)
  return results
}

/**
 * @public
 *
 * Reads the up files of scanned migrations, leaving each down file to be read only if a rollback needs it.
 *
 * @param scanned - Migrations found by {@link scanDirectory}.
 * @returns The migrations, in ascending version order.
 */
export function readUpMigrations(scanned: ScannedMigration[]): Migration[] {
  return scanned.map(entry => {
    const sql = readFileSync(entry.upPath, 'utf-8').trim()
    if (sql.length === 0) {
      throw new MigrationError(`Migration file is empty: ${entry.upPath}`, entry.version, 'MIGRATION_VALIDATION_ERROR')
    }
    return {
      version: entry.version,
      name: entry.name,
      up: sql,
    }
  })
}

function readDownSql(downPath: string, version: number): string {
  let contents: string
  try {
    contents = readFileSync(downPath, 'utf-8')
  } catch (err) {
    if (err && typeof err === 'object' && 'code' in err && (err as { code?: unknown }).code === 'ENOENT') {
      throw new MigrationError(`Down migration file no longer exists: ${downPath}`, version, 'MIGRATION_NO_DOWN')
    }
    throw new MigrationError(
      `Failed to read down migration file ${downPath}: ${err instanceof Error ? err.message : String(err)}`,
      version,
      'MIGRATION_ROLLBACK_ERROR',
    )
  }

  const downSql = contents.trim()
  if (downSql.length === 0) {
    throw new MigrationError(`Down migration file is empty: ${downPath}`, version, 'MIGRATION_VALIDATION_ERROR')
  }
  return downSql
}

function attachLazyDown(migration: Migration, downPath: string): void {
  Object.defineProperty(migration, LAZY_DOWN_SQL, {
    value: () => readDownSql(downPath, migration.version),
    enumerable: false,
    configurable: false,
    writable: false,
  })
}

/**
 * @public
 *
 * Reads a directory of migration files and returns the migrations to apply.
 *
 * @param dirPath - Directory holding the migration files.
 * @param options - The baseline to apply, when an existing database starts from one.
 * @returns The migrations, in ascending version order.
 */
export function loadMigrations(dirPath: string, options?: LoadMigrationsOptions): Migration[] {
  const scanned = scanDirectory(dirPath)
  const migrations = readUpMigrations(scanned)
  for (let i = 0; i < migrations.length; i++) {
    const downPath = scanned[i].downPath
    if (downPath !== null) {
      attachLazyDown(migrations[i], downPath)
    }
  }
  return applyBaselineOption(migrations, options?.baseline)
}
