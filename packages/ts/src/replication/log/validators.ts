export const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export const SAFE_DDL_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

export const DDL_DENY_RE = /\b(load_extension|ATTACH|randomblob|zeroblob|writefile|readfile|fts3_tokenizer)\b/i

export function validateIdentifier(name: string): boolean {
  return IDENTIFIER_RE.test(name)
}

export function validateDdlSafety(sql: string): boolean {
  if (!SAFE_DDL_RE.test(sql)) return false
  if (sql.includes(';')) return false
  if (/\bAS\s+SELECT\b/i.test(sql)) return false
  if (DDL_DENY_RE.test(sql)) return false
  const body = sql.replace(SAFE_DDL_RE, '')
  if (/\bSELECT\b/i.test(body)) return false
  return true
}
