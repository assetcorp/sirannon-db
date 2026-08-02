import { MIGRATIONS_TABLE } from '../internal-tables.js'

const BUSY_CODES = new Set(['SQLITE_BUSY', 'SQLITE_BUSY_SNAPSHOT', 'SQLITE_BUSY_RECOVERY', 'SQLITE_BUSY_TIMEOUT'])

export function isConcurrentMigrationConflict(err: unknown): boolean {
  if (!(err instanceof Error)) return false

  const code = (err as { code?: unknown }).code
  if (typeof code === 'string' && BUSY_CODES.has(code)) return true

  const message = err.message
  if (/SQLITE_BUSY|database is locked/i.test(message)) return true
  return message.includes(`UNIQUE constraint failed: ${MIGRATIONS_TABLE}.version`)
}
