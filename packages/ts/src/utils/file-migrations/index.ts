import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { MigrationError } from '../../core/errors.js'
import { parseMigrationFilename } from '../../core/migrations/filename.js'
import { LAZY_DOWN_SQL } from '../../core/migrations/lazy-down.js'
import type { Migration } from '../../core/migrations/types.js'

export interface ScannedMigration {
  version: number
  name: string
  upPath: string
  downPath: string | null
}

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    if (s.charCodeAt(i) <= 0x1f) return true
  }
  return false
}

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
  const downSql = readFileSync(downPath, 'utf-8').trim()
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

export function loadMigrations(dirPath: string): Migration[] {
  const scanned = scanDirectory(dirPath)
  const migrations = readUpMigrations(scanned)
  for (let i = 0; i < migrations.length; i++) {
    const downPath = scanned[i].downPath
    if (downPath !== null) {
      attachLazyDown(migrations[i], downPath)
    }
  }
  return migrations
}
