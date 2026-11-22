import { ForbiddenSqlError } from './errors.js'

export const INTERNAL_TABLE_PREFIX = '_sirannon_'

export const CHANGES_TABLE = '_sirannon_changes'
export const META_TABLE = '_sirannon_meta'
export const MIGRATIONS_TABLE = '_sirannon_migrations'
export const PEER_STATE_TABLE = '_sirannon_peer_state'
export const APPLIED_CHANGES_TABLE = '_sirannon_applied_changes'
export const COLUMN_VERSIONS_TABLE = '_sirannon_column_versions'
export const SYNC_STATE_TABLE = '_sirannon_sync_state'
export const DEVICE_CURSORS_TABLE = '_sirannon_device_cursors'
export const STAGED_CHANGES_TABLE = '_sirannon_staged_changes'
export const STAGED_PUSH_CHANGES_TABLE = '_sirannon_staged_push_changes'
export const REPLACED_ROWS_TABLE = '_sirannon_replaced_rows'
export const ROLLED_BACK_CHANGES_TABLE = '_sirannon_rolled_back_changes'

export const CDC_TRIGGER_PREFIX = '_sirannon_trg_'
export const LIVE_PROBE_TABLE_PREFIX = '_sirannon_live_probe_'
export const KEY_CHANGE_CAPTURE_PREFIX = '_sirannon_key_change_'

const SIRANNON_PREFIX = '_sirannon'
const SQLITE_PREFIX = 'sqlite_'

const SIRANNON_MESSAGE =
  "Access to internal tables is not permitted: identifiers beginning with '_sirannon' are reserved"
const SQLITE_WRITE_MESSAGE =
  "The SQLite schema catalogue is read-only: statements that modify a 'sqlite_' identifier are not permitted"
const ATTACH_MESSAGE = 'ATTACH and DETACH statements are not permitted'
const VACUUM_INTO_MESSAGE = 'VACUUM INTO is not permitted'
const FORBIDDEN_PRAGMA_MESSAGES = new Map([
  ['writable_schema', 'PRAGMA writable_schema is not permitted'],
  ['temp_store_directory', 'PRAGMA temp_store_directory is not permitted'],
  ['data_store_directory', 'PRAGMA data_store_directory is not permitted'],
  [
    'wal_checkpoint',
    'PRAGMA wal_checkpoint is not permitted through query or execute: call checkpoint() on the database, which runs the checkpoint on the writer and reports whether a reader stopped it',
  ],
])

const WRITE_VERBS = new Set(['insert', 'update', 'delete', 'replace', 'create', 'alter', 'drop', 'vacuum', 'reindex'])
const STATEMENT_VERBS_AFTER_WITH = new Set(['select', 'insert', 'update', 'delete', 'replace'])

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
 * Returns the reason that the query API refuses a statement, or `null` when the query API accepts it.
 *
 * The query API refuses a statement that names a `_sirannon` identifier, modifies a `sqlite_` identifier, or uses
 * `ATTACH`, `DETACH`, `VACUUM INTO`, `PRAGMA writable_schema`, `PRAGMA temp_store_directory`,
 * `PRAGMA data_store_directory`, or `PRAGMA wal_checkpoint`. A pragma's name counts whether the statement writes it bare, quoted as an identifier,
 * or as a string literal.
 *
 * @param sql - The statement text, which can hold several statements separated by semicolons.
 * @returns The reason for the first refused statement, or `null` when the guard refuses none of them.
 * @internal
 */
export function reservedSqlError(sql: string): string | null {
  const lower = sql.toLowerCase()
  if (
    !lower.includes('_sirannon') &&
    !lower.includes('sqlite_') &&
    !lower.includes('attach') &&
    !lower.includes('detach') &&
    !lower.includes('writable_schema') &&
    !lower.includes('vacuum') &&
    !lower.includes('_store_directory') &&
    !lower.includes('wal_checkpoint')
  ) {
    return null
  }
  return scan(sql)
}

/**
 * Returns normally when the query API accepts a statement, and throws when {@link reservedSqlError} gives a reason to
 * refuse it.
 *
 * @param sql - The statement text, which can hold several statements separated by semicolons.
 * @throws {@link ForbiddenSqlError} with the code `FORBIDDEN_SQL` when the query API refuses the statement.
 * @internal
 */
export function assertSqlAllowed(sql: string): void {
  const reason = reservedSqlError(sql)
  if (reason) {
    throw new ForbiddenSqlError(reason)
  }
}

function isWordStart(c: string): boolean {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_' || c >= '\u0080'
}

function isWordPart(c: string): boolean {
  return isWordStart(c) || (c >= '0' && c <= '9') || c === '$'
}

function referenceError(kind: ReservedKind, verb: string): string | null {
  if (kind === 'sirannon') return SIRANNON_MESSAGE
  if (kind === 'sqlite' && WRITE_VERBS.has(verb)) return SQLITE_WRITE_MESSAGE
  return null
}

interface StatementState {
  verb: string
  depth: number
  namesCatalogue: boolean
  pragmaValue: boolean
}

function freshStatement(): StatementState {
  return { verb: '', depth: 0, namesCatalogue: false, pragmaValue: false }
}

function pragmaError(name: string, state: StatementState): string | null {
  if (state.verb !== 'pragma' || state.pragmaValue) return null
  return FORBIDDEN_PRAGMA_MESSAGES.get(name.toLowerCase()) ?? null
}

function identifierError(name: string, state: StatementState): string | null {
  const forbiddenPragma = pragmaError(name, state)
  if (forbiddenPragma) return forbiddenPragma
  const kind = reservedKind(name)
  if (kind === 'sqlite') state.namesCatalogue = true
  return referenceError(kind, state.verb)
}

function wordError(word: string, statementStart: boolean, state: StatementState): string | null {
  const lowerWord = word.toLowerCase()
  if ((state.verb === 'explain' && lowerWord === 'query') || (state.verb === 'explain query' && lowerWord === 'plan')) {
    state.verb += ` ${lowerWord}`
    return null
  }
  if (statementStart || state.verb === 'explain' || state.verb === 'explain query plan') {
    state.verb = lowerWord
    if (lowerWord === 'attach' || lowerWord === 'detach') return ATTACH_MESSAGE
    return identifierError(word, state)
  }
  if (state.verb === 'vacuum' && lowerWord === 'into') return VACUUM_INTO_MESSAGE
  if (state.verb === 'with' && state.depth === 0 && STATEMENT_VERBS_AFTER_WITH.has(lowerWord)) {
    state.verb = lowerWord
    return state.namesCatalogue && WRITE_VERBS.has(lowerWord) ? SQLITE_WRITE_MESSAGE : null
  }
  return identifierError(word, state)
}

function scan(sql: string): string | null {
  const n = sql.length
  let i = 0
  let statementStart = true
  const state = freshStatement()

  while (i < n) {
    const c = sql[i]

    if (c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v' || c === '\ufeff') {
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
      if (state.verb === 'pragma' && !state.pragmaValue) {
        const literal = readDelimited(sql, i, "'")
        const error = pragmaError(literal.value, state)
        if (error) return error
        i = literal.next
      } else {
        i = skipQuoted(sql, i, "'")
      }
      statementStart = false
      continue
    }

    if (c === '"' || c === '`') {
      const delimited = readDelimited(sql, i, c)
      const error = identifierError(delimited.value, state)
      if (error) return error
      i = delimited.next
      statementStart = false
      continue
    }

    if (c === '[') {
      const end = sql.indexOf(']', i + 1)
      const stop = end === -1 ? n : end
      const error = identifierError(sql.slice(i + 1, stop), state)
      if (error) return error
      i = stop + 1
      statementStart = false
      continue
    }

    if (c === ';') {
      i++
      statementStart = true
      Object.assign(state, freshStatement())
      continue
    }

    if (isWordStart(c)) {
      let j = i + 1
      while (j < n && isWordPart(sql[j])) j++
      const error = wordError(sql.slice(i, j), statementStart, state)
      if (error) return error
      i = j
      statementStart = false
      continue
    }

    if (state.verb === 'pragma' && (c === '(' || c === '=')) state.pragmaValue = true
    if (c === '(') state.depth++
    else if (c === ')' && state.depth > 0) state.depth--
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
