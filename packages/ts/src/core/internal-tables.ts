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

const SIRANNON_PREFIX = '_sirannon'
const SQLITE_PREFIX = 'sqlite_'

const SIRANNON_MESSAGE =
  "Access to internal tables is not permitted: identifiers beginning with '_sirannon' are reserved"
const SQLITE_WRITE_MESSAGE =
  "The SQLite schema catalogue is read-only: statements that modify a 'sqlite_' identifier are not permitted"
const ATTACH_MESSAGE = 'ATTACH and DETACH statements are not permitted'
const WRITABLE_SCHEMA_MESSAGE = 'PRAGMA writable_schema is not permitted'

const WRITE_VERBS = new Set(['insert', 'update', 'delete', 'replace', 'create', 'alter', 'drop', 'vacuum', 'reindex'])

type ReservedKind = 'sirannon' | 'sqlite' | null

function reservedKind(name: string): ReservedKind {
  const lower = name.toLowerCase()
  if (lower.startsWith(SIRANNON_PREFIX)) return 'sirannon'
  if (lower.startsWith(SQLITE_PREFIX)) return 'sqlite'
  return null
}

export function isReservedIdentifier(name: string): boolean {
  return reservedKind(name) !== null
}

/**
 * Reports why a statement is refused, or null when it is allowed. Sirannon's
 * own bookkeeping runs on raw connections that never reach this check, so the
 * reserved namespace is closed to the query API without blocking the engine
 * from maintaining its own tables.
 *
 * The `_sirannon_` tables are Sirannon's private ledger (deleted rows, prior
 * values, replication state), so any reference is refused. The `sqlite_`
 * catalogue is readable, matching every SQL engine, but statements that would
 * modify it are refused; SQLite already treats the catalogue as read-only
 * unless `PRAGMA writable_schema` is set, which is refused here too.
 */
export function reservedSqlError(sql: string): string | null {
  const lower = sql.toLowerCase()
  if (
    !lower.includes('_sirannon') &&
    !lower.includes('sqlite_') &&
    !lower.includes('attach') &&
    !lower.includes('detach') &&
    !lower.includes('writable_schema')
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

function referenceError(kind: ReservedKind, verb: string): string | null {
  if (kind === 'sirannon') return SIRANNON_MESSAGE
  if (kind === 'sqlite' && WRITE_VERBS.has(verb)) return SQLITE_WRITE_MESSAGE
  return null
}

function scan(sql: string): string | null {
  const n = sql.length
  let i = 0
  let statementStart = true
  let verb = ''

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
      const error = referenceError(reservedKind(delimited.value), verb)
      if (error) return error
      i = delimited.next
      statementStart = false
      continue
    }

    if (c === '[') {
      const end = sql.indexOf(']', i + 1)
      const stop = end === -1 ? n : end
      const error = referenceError(reservedKind(sql.slice(i + 1, stop)), verb)
      if (error) return error
      i = stop + 1
      statementStart = false
      continue
    }

    if (c === ';') {
      i++
      statementStart = true
      verb = ''
      continue
    }

    if (isWordStart(c)) {
      let j = i + 1
      while (j < n && isWordPart(sql[j])) j++
      const word = sql.slice(i, j)
      const lowerWord = word.toLowerCase()
      if (statementStart) {
        verb = lowerWord
        if (lowerWord === 'attach' || lowerWord === 'detach') return ATTACH_MESSAGE
      } else if (verb === 'pragma' && lowerWord === 'writable_schema') {
        return WRITABLE_SCHEMA_MESSAGE
      }
      const error = referenceError(reservedKind(word), verb)
      if (error) return error
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
