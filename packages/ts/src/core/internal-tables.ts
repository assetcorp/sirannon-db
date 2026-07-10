import { ForbiddenSqlError } from './errors.js'

export const INTERNAL_TABLE_PREFIX = '_sirannon_'

export const CHANGES_TABLE = '_sirannon_changes'
export const META_TABLE = '_sirannon_meta'
export const MIGRATIONS_TABLE = '_sirannon_migrations'
export const PEER_STATE_TABLE = '_sirannon_peer_state'
export const APPLIED_CHANGES_TABLE = '_sirannon_applied_changes'
export const COLUMN_VERSIONS_TABLE = '_sirannon_column_versions'
export const SYNC_STATE_TABLE = '_sirannon_sync_state'

export const CDC_TRIGGER_PREFIX = '_sirannon_trg_'

const RESERVED_PREFIXES = ['_sirannon', 'sqlite_']

const RESERVED_MESSAGE =
  "Access to internal tables is not permitted: identifiers beginning with '_sirannon' or 'sqlite_' are reserved"
const ATTACH_MESSAGE = 'ATTACH and DETACH statements are not permitted'

export function isReservedIdentifier(name: string): boolean {
  const lower = name.toLowerCase()
  for (const prefix of RESERVED_PREFIXES) {
    if (lower.startsWith(prefix)) return true
  }
  return false
}

/**
 * Reports why a statement is refused, or null when it is allowed. Sirannon's
 * own bookkeeping runs on raw connections that never reach this check, so the
 * reserved namespace is closed to the query API without blocking the engine
 * from maintaining its own tables.
 */
export function reservedSqlError(sql: string): string | null {
  const lower = sql.toLowerCase()
  if (
    !lower.includes('_sirannon') &&
    !lower.includes('sqlite_') &&
    !lower.includes('attach') &&
    !lower.includes('detach')
  ) {
    return null
  }
  return scan(sql)
}

export function assertSqlAllowed(sql: string): void {
  const reason = reservedSqlError(sql)
  if (reason) {
    throw new ForbiddenSqlError(reason)
  }
}

function isWordStart(c: string): boolean {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_'
}

function isWordPart(c: string): boolean {
  return isWordStart(c) || (c >= '0' && c <= '9') || c === '$'
}

function scan(sql: string): string | null {
  const n = sql.length
  let i = 0
  let statementStart = true

  while (i < n) {
    const c = sql[i]

    if (c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v') {
      i++
      continue
    }

    if (c === '-' && sql[i + 1] === '-') {
      i += 2
      while (i < n && sql[i] !== '\n') i++
      continue
    }

    if (c === '/' && sql[i + 1] === '*') {
      i += 2
      while (i < n && !(sql[i] === '*' && sql[i + 1] === '/')) i++
      i += 2
      continue
    }

    if (c === "'") {
      i = skipQuoted(sql, i, "'")
      statementStart = false
      continue
    }

    if (c === '"' || c === '`') {
      const delimited = readDelimited(sql, i, c)
      if (isReservedIdentifier(delimited.value)) return RESERVED_MESSAGE
      i = delimited.next
      statementStart = false
      continue
    }

    if (c === '[') {
      const end = sql.indexOf(']', i + 1)
      const stop = end === -1 ? n : end
      if (isReservedIdentifier(sql.slice(i + 1, stop))) return RESERVED_MESSAGE
      i = stop + 1
      statementStart = false
      continue
    }

    if (c === ';') {
      i++
      statementStart = true
      continue
    }

    if (isWordStart(c)) {
      let j = i + 1
      while (j < n && isWordPart(sql[j])) j++
      const word = sql.slice(i, j)
      const lowerWord = word.toLowerCase()
      if (statementStart && (lowerWord === 'attach' || lowerWord === 'detach')) {
        return ATTACH_MESSAGE
      }
      if (isReservedIdentifier(word)) return RESERVED_MESSAGE
      i = j
      statementStart = false
      continue
    }

    i++
    statementStart = false
  }

  return null
}

function skipQuoted(sql: string, start: number, quote: string): number {
  const n = sql.length
  let i = start + 1
  while (i < n) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) {
        i += 2
        continue
      }
      return i + 1
    }
    i++
  }
  return n
}

function readDelimited(sql: string, start: number, quote: string): { value: string; next: number } {
  const n = sql.length
  let i = start + 1
  let value = ''
  while (i < n) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) {
        value += quote
        i += 2
        continue
      }
      return { value, next: i + 1 }
    }
    value += sql[i]
    i++
  }
  return { value, next: n }
}
