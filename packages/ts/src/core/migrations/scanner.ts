import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { MigrationError } from '../errors.js'
import type { Migration } from './types.js'

export interface ScannedMigration {
  version: number
  name: string
  upPath: string
  downPath: string | null
}

const MIGRATION_FILENAME_PATTERN = /^(\d+)_(\w+)\.(up|down)\.sql$/

export function scanDirectory(dirPath: string): ScannedMigration[] {
  if (dirPath.includes('\0')) {
    throw new MigrationError('Migration path contains null bytes', 0, 'MIGRATION_VALIDATION_ERROR')
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

    const match = MIGRATION_FILENAME_PATTERN.exec(entry.name)
    if (!match) continue

    const version = parseInt(match[1], 10)
    const name = match[2]
    const direction = match[3] as 'up' | 'down'

    if (!Number.isFinite(version) || version <= 0 || !Number.isSafeInteger(version)) {
      throw new MigrationError(`Invalid migration version: ${match[1]}`, version, 'MIGRATION_VALIDATION_ERROR')
    }

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

export function readDownMigrations(scanned: ScannedMigration[], versions: number[]): Migration[] {
  const versionSet = new Set(versions)
  const filtered = scanned.filter(s => versionSet.has(s.version))

  return filtered.map(entry => {
    if (!entry.downPath) {
      throw new MigrationError(
        `Migration version ${entry.version} (${entry.name}) has no .down.sql file`,
        entry.version,
        'MIGRATION_NO_DOWN',
      )
    }

    const downSql = readFileSync(entry.downPath, 'utf-8').trim()
    if (downSql.length === 0) {
      throw new MigrationError(
        `Down migration file is empty: ${entry.downPath}`,
        entry.version,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    return {
      version: entry.version,
      name: entry.name,
      up: '',
      down: downSql,
    }
  })
}
