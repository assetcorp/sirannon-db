import { isReservedIdentifier } from '../internal-tables.js'

export const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export const SEQ_STRING_RE = /^\d{1,19}$/

export const SAFE_DDL_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

export const DDL_DENY_RE = /\b(load_extension|ATTACH|randomblob|zeroblob|writefile|readfile|fts3_tokenizer)\b/i

export function validateIdentifier(name: string): boolean {
  return IDENTIFIER_RE.test(name)
}

const CREATE_INDEX_TARGETS_RE =
  /^\s*CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+(?:["`[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?\s*\.\s*)?["`[]?([A-Za-z_][A-Za-z0-9_]*)["`\]]?\s+ON\s+(?:["`[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?\s*\.\s*)?["`[]?([A-Za-z_][A-Za-z0-9_]*)/i

const DDL_TARGET_RE =
  /^\s*(?:CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|ALTER\s+TABLE|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|DROP\s+INDEX(?:\s+IF\s+EXISTS)?)\s+(?:["`[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?\s*\.\s*)?["`[]?([A-Za-z_][A-Za-z0-9_]*)/i

/**
 * Returns `true` when a DDL statement targets a table or index whose name Sirannon reserves.
 *
 * The function checks only the name of the table or index that the statement
 * targets, so it returns `false` for an added column whose name has a reserved
 * prefix, and `true` for a statement against `_sirannon_changes` or a `sqlite_`
 * table.
 *
 * @param sql - The DDL statement to check.
 * @returns `true` when the statement targets a reserved table or index.
 */
export function ddlTargetsReservedIdentifier(sql: string): boolean {
  const indexTargets = CREATE_INDEX_TARGETS_RE.exec(sql)
  if (indexTargets !== null) {
    return isReservedIdentifier(indexTargets[1]) || isReservedIdentifier(indexTargets[2])
  }

  const target = DDL_TARGET_RE.exec(sql)
  if (target !== null) return isReservedIdentifier(target[1])

  return false
}

export function validateDdlSafety(sql: string): boolean {
  if (ddlTargetsReservedIdentifier(sql)) return false
  if (!SAFE_DDL_RE.test(sql)) return false
  if (sql.includes(';')) return false
  if (/\bAS\s+SELECT\b/i.test(sql)) return false
  if (DDL_DENY_RE.test(sql)) return false
  const body = sql.replace(SAFE_DDL_RE, '')
  if (/\bSELECT\b/i.test(body)) return false
  return true
}

const DROP_TABLE_RE = /^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

/**
 * Returns the table name from a `DROP TABLE` statement, or `null` when the SQL
 * is another statement or names the table in a form that the pattern rejects.
 *
 * The local executor and the batch applier collect these names so that the
 * caller can prune each dropped table from the `ChangeTracker` after the
 * transaction commits. The pattern accepts an optional `IF EXISTS`, double
 * quotes around the name, and one trailing semicolon. It returns `null` for a
 * schema-qualified name or any other trailing text, in which case the tracker
 * keeps its entry for that table.
 */
export function extractDroppedTable(sql: string): string | null {
  const m = DROP_TABLE_RE.exec(sql)
  return m?.[1] ?? null
}
