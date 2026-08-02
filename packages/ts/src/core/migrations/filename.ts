import { MigrationError } from '../errors.js'

export const MIGRATION_FILENAME_PATTERN = /^(\d+)_(\w+)\.(up|down)\.sql$/

export interface ParsedMigrationFilename {
  version: number
  name: string
  direction: 'up' | 'down'
}

export function parseMigrationFilename(filename: string): ParsedMigrationFilename | null {
  const match = MIGRATION_FILENAME_PATTERN.exec(filename)
  if (!match) return null

  const version = parseInt(match[1], 10)
  if (!Number.isFinite(version) || version <= 0 || !Number.isSafeInteger(version)) {
    throw new MigrationError(`Invalid migration version: ${match[1]}`, version, 'MIGRATION_VALIDATION_ERROR')
  }

  return { version, name: match[2], direction: match[3] as 'up' | 'down' }
}
