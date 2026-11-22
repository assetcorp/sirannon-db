import { ForbiddenSqlError } from './errors.js'
import { type ReservedIdentifierKind, reservedIdentifierKind } from './internal-tables.js'
import {
  isHeadWord,
  namesTemporaryObject,
  STATEMENT_HEAD_TOKENS,
  type StatementHeadToken,
} from './sql-statement-head.js'
import {
  isWordPart,
  isWordStart,
  namedParameterEnd,
  opensNamedParameter,
  readDelimited,
  skipQuoted,
} from './sql-tokens.js'

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

type GuardScope = 'query' | 'migration'

interface ScopeMessages {
  transactionControl: string
  temporaryObject: string
}

const SCOPE_MESSAGES: Record<GuardScope, ScopeMessages> = {
  query: {
    transactionControl:
      'BEGIN, COMMIT, END, ROLLBACK, SAVEPOINT, and RELEASE statements are not permitted: call transaction() or executeTransaction() on the database, which opens the transaction, commits it when every statement succeeds, and rolls it back when one fails',
    temporaryObject:
      'Creating or dropping a temporary table, view, index, or trigger is not permitted, because the object would stay on a connection that later callers share: inside transaction(), create a regular table and drop it before your function returns, or name the rows with a WITH clause',
  },
  migration: {
    transactionControl:
      'BEGIN, COMMIT, END, ROLLBACK, SAVEPOINT, and RELEASE statements are not permitted in a migration: Sirannon runs each migration in a separate transaction, which it rolls back when a statement fails',
    temporaryObject:
      'Creating or dropping a temporary table, view, index, or trigger is not permitted in a migration, because the object would stay on the writer connection after the migration: create a regular table and drop it before the migration ends',
  },
}

const MIGRATION_RULE_WORDS = ['begin', 'commit', 'end', 'rollback', 'savepoint', 'release', 'temp']
const QUERY_RULE_WORDS = [
  ...MIGRATION_RULE_WORDS,
  '_sirannon',
  'sqlite_',
  'attach',
  'detach',
  'writable_schema',
  'vacuum',
  '_store_directory',
  'wal_checkpoint',
]

const WRITE_VERBS = new Set(['insert', 'update', 'delete', 'replace', 'create', 'alter', 'drop', 'vacuum', 'reindex'])
const STATEMENT_VERBS_AFTER_WITH = new Set(['select', 'insert', 'update', 'delete', 'replace'])
const TRANSACTION_CONTROL_VERBS = new Set(['begin', 'commit', 'end', 'rollback', 'savepoint', 'release'])
const BYTE_ORDER_MARK = String.fromCharCode(0xfeff)

function holdsAnyWord(sql: string, words: readonly string[]): boolean {
  const lower = sql.toLowerCase()
  return words.some(word => lower.includes(word))
}

/**
 * Returns the reason that the query API refuses a statement, or `null` when the query API accepts it.
 *
 * The query API refuses a statement that names a `_sirannon` identifier, modifies a `sqlite_` identifier, or uses
 * `ATTACH`, `DETACH`, `VACUUM INTO`, `PRAGMA writable_schema`, `PRAGMA temp_store_directory`,
 * `PRAGMA data_store_directory`, or `PRAGMA wal_checkpoint`. A pragma's name counts whether the statement writes it bare, quoted as an identifier,
 * or as a string literal. It also refuses what {@link migrationSqlError} refuses: a statement that begins, ends, or
 * marks a transaction, and one that creates or drops a temporary object.
 *
 * @param sql - The statement text, which can hold several statements separated by semicolons.
 * @returns The reason for the first refused statement, or `null` when the guard refuses none of them.
 * @internal
 */
export function reservedSqlError(sql: string): string | null {
  return holdsAnyWord(sql, QUERY_RULE_WORDS) ? scan(sql, 'query') : null
}

/**
 * Returns the reason that Sirannon refuses the SQL text of a migration, or `null` when it accepts the text.
 *
 * A migration's text may not hold a `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, or `RELEASE` statement, because
 * Sirannon runs each migration in a separate transaction. It may not create or drop a temporary table, view, index, or
 * trigger, whether through `TEMP`, `TEMPORARY`, or the `temp` schema, because that object would stay on the writer
 * connection. The `END` that closes a `CREATE TRIGGER` body counts as part of that statement.
 *
 * @param sql - The migration's text, which can hold several statements separated by semicolons.
 * @returns The reason for the first refused statement, or `null` when the guard refuses none of them.
 * @internal
 */
export function migrationSqlError(sql: string): string | null {
  return holdsAnyWord(sql, MIGRATION_RULE_WORDS) ? scan(sql, 'migration') : null
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

/**
 * Returns normally when Sirannon accepts a migration's SQL text, and throws when {@link migrationSqlError} gives a
 * reason to refuse it.
 *
 * @param sql - The migration's text, which can hold several statements separated by semicolons.
 * @throws {@link ForbiddenSqlError} with the code `FORBIDDEN_SQL` when Sirannon refuses the text.
 * @internal
 */
export function assertMigrationSqlAllowed(sql: string): void {
  const reason = migrationSqlError(sql)
  if (reason) {
    throw new ForbiddenSqlError(reason)
  }
}

interface StatementState {
  scope: GuardScope
  verb: string
  depth: number
  namesCatalogue: boolean
  pragmaValue: boolean
  head: StatementHeadToken[]
  createsTrigger: boolean
  triggerBody: boolean
}

function freshStatement(scope: GuardScope, triggerBody: boolean): StatementState {
  return {
    scope,
    verb: '',
    depth: 0,
    namesCatalogue: false,
    pragmaValue: false,
    head: [],
    createsTrigger: false,
    triggerBody,
  }
}

function queryRuleError(message: string | null, state: StatementState): string | null {
  return state.scope === 'query' ? message : null
}

function referenceError(kind: ReservedIdentifierKind, verb: string): string | null {
  if (kind === 'sirannon') return SIRANNON_MESSAGE
  if (kind === 'sqlite' && WRITE_VERBS.has(verb)) return SQLITE_WRITE_MESSAGE
  return null
}

function pragmaError(name: string, state: StatementState): string | null {
  if (state.verb !== 'pragma' || state.pragmaValue) return null
  return queryRuleError(FORBIDDEN_PRAGMA_MESSAGES.get(name.toLowerCase()) ?? null, state)
}

function identifierError(name: string, state: StatementState): string | null {
  const forbiddenPragma = pragmaError(name, state)
  if (forbiddenPragma) return forbiddenPragma
  const kind = reservedIdentifierKind(name)
  if (kind === 'sqlite') state.namesCatalogue = true
  return queryRuleError(referenceError(kind, state.verb), state)
}

function collectsHead(state: StatementState): boolean {
  return state.depth === 0 && state.head.length < STATEMENT_HEAD_TOKENS
}

function headError(token: StatementHeadToken, state: StatementState): string | null {
  if (!collectsHead(state)) return null
  state.head.push(token)
  if (state.head.length === 2 && isHeadWord(state.head[0], 'create') && isHeadWord(token, 'trigger')) {
    state.createsTrigger = true
  }
  return namesTemporaryObject(state.head) ? SCOPE_MESSAGES[state.scope].temporaryObject : null
}

function transactionControlError(verb: string, state: StatementState): string | null {
  if (verb === 'end' && state.triggerBody) {
    state.triggerBody = false
    return null
  }
  return SCOPE_MESSAGES[state.scope].transactionControl
}

function wordError(word: string, statementStart: boolean, state: StatementState): string | null {
  const lowerWord = word.toLowerCase()
  if ((state.verb === 'explain' && lowerWord === 'query') || (state.verb === 'explain query' && lowerWord === 'plan')) {
    state.verb += ` ${lowerWord}`
    return null
  }
  if (statementStart || state.verb === 'explain' || state.verb === 'explain query plan') {
    state.verb = lowerWord
    state.head = [{ kind: 'word', text: lowerWord }]
    if (TRANSACTION_CONTROL_VERBS.has(lowerWord)) return transactionControlError(lowerWord, state)
    if (lowerWord === 'attach' || lowerWord === 'detach') return queryRuleError(ATTACH_MESSAGE, state)
    return identifierError(word, state)
  }
  const temporaryObject = headError({ kind: 'word', text: lowerWord }, state)
  if (temporaryObject) return temporaryObject
  if (state.createsTrigger && !state.triggerBody && state.depth === 0 && lowerWord === 'begin') {
    state.triggerBody = true
  }
  if (state.verb === 'vacuum' && lowerWord === 'into') return queryRuleError(VACUUM_INTO_MESSAGE, state)
  if (state.verb === 'with' && state.depth === 0 && STATEMENT_VERBS_AFTER_WITH.has(lowerWord)) {
    state.verb = lowerWord
    return state.namesCatalogue && WRITE_VERBS.has(lowerWord) ? queryRuleError(SQLITE_WRITE_MESSAGE, state) : null
  }
  return identifierError(word, state)
}

function quotedNameError(name: string, state: StatementState): string | null {
  return identifierError(name, state) ?? headError({ kind: 'name', text: name.toLowerCase() }, state)
}

function scan(sql: string, scope: GuardScope): string | null {
  const n = sql.length
  let i = 0
  let statementStart = true
  const state = freshStatement(scope, false)

  while (i < n) {
    const c = sql[i]

    if (c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v' || c === BYTE_ORDER_MARK) {
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
      const namesPragma = state.verb === 'pragma' && !state.pragmaValue
      if (!namesPragma && !collectsHead(state)) {
        i = skipQuoted(sql, i, "'")
        statementStart = false
        continue
      }
      const literal = readDelimited(sql, i, "'")
      const error = namesPragma
        ? pragmaError(literal.value, state)
        : headError({ kind: 'name', text: literal.value.toLowerCase() }, state)
      if (error) return error
      i = literal.next
      statementStart = false
      continue
    }

    if (c === '"' || c === '`') {
      const delimited = readDelimited(sql, i, c)
      const error = quotedNameError(delimited.value, state)
      if (error) return error
      i = delimited.next
      statementStart = false
      continue
    }

    if (c === '[') {
      const end = sql.indexOf(']', i + 1)
      const stop = end === -1 ? n : end
      const error = quotedNameError(sql.slice(i + 1, stop), state)
      if (error) return error
      i = stop + 1
      statementStart = false
      continue
    }

    if (c === ';') {
      i++
      statementStart = true
      Object.assign(state, freshStatement(state.scope, state.triggerBody))
      continue
    }

    if (opensNamedParameter(c)) {
      const parameterError = headError({ kind: 'other', text: c }, state)
      if (parameterError) return parameterError
      i = namedParameterEnd(sql, i)
      statementStart = false
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

    const symbolError = headError({ kind: c === '.' ? 'dot' : 'other', text: c }, state)
    if (symbolError) return symbolError
    if (state.verb === 'pragma' && (c === '(' || c === '=')) state.pragmaValue = true
    if (c === '(') state.depth++
    else if (c === ')' && state.depth > 0) state.depth--
    i++
    statementStart = false
  }

  return null
}
