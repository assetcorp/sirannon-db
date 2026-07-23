import { MigrationError } from '../errors.js'
import { applyBaselineOption, type BaselineFileOption } from './baseline.js'
import { parseMigrationFilename } from './filename.js'
import type { Migration } from './types.js'

export interface MigrationsFromFilesOptions {
  baseline?: BaselineFileOption
}

interface GroupedMigrationFiles {
  name: string
  up?: string
  down?: string
}

function basename(key: string): string {
  const separator = Math.max(key.lastIndexOf('/'), key.lastIndexOf('\\'))
  return separator === -1 ? key : key.slice(separator + 1)
}

export function migrationsFromFiles(files: Record<string, unknown>, options?: MigrationsFromFilesOptions): Migration[] {
  const grouped = new Map<number, GroupedMigrationFiles>()

  for (const [key, contents] of Object.entries(files)) {
    const parsed = parseMigrationFilename(basename(key))
    if (!parsed) {
      throw new MigrationError(
        `Migration file '${key}' does not match the <version>_<name>.up.sql / <version>_<name>.down.sql convention`,
        0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    if (typeof contents !== 'string') {
      throw new MigrationError(
        `Migration file '${key}' must map to its SQL text; received ${typeof contents}. When bundling with import.meta.glob, pass { query: '?raw', import: 'default', eager: true }`,
        parsed.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    const sql = contents.trim()
    if (sql.length === 0) {
      throw new MigrationError(`Migration file is empty: ${key}`, parsed.version, 'MIGRATION_VALIDATION_ERROR')
    }

    const existing = grouped.get(parsed.version)
    if (!existing) {
      grouped.set(parsed.version, { name: parsed.name, [parsed.direction]: sql })
      continue
    }

    if (existing.name !== parsed.name) {
      throw new MigrationError(
        `Duplicate migration version ${parsed.version}: '${existing.name}' and '${parsed.name}'`,
        parsed.version,
        'MIGRATION_DUPLICATE_VERSION',
      )
    }

    if (existing[parsed.direction] !== undefined) {
      throw new MigrationError(
        `Duplicate ${parsed.direction} migration for version ${parsed.version} ('${parsed.name}')`,
        parsed.version,
        'MIGRATION_DUPLICATE_VERSION',
      )
    }

    existing[parsed.direction] = sql
  }

  const migrations: Migration[] = []
  for (const [version, entry] of grouped) {
    if (entry.up === undefined) {
      throw new MigrationError(
        `Migration version ${version} (${entry.name}) is missing an .up.sql file`,
        version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }
    migrations.push(
      entry.down === undefined
        ? { version, name: entry.name, up: entry.up }
        : { version, name: entry.name, up: entry.up, down: entry.down },
    )
  }

  migrations.sort((a, b) => a.version - b.version)
  return applyBaselineOption(migrations, options?.baseline)
}
