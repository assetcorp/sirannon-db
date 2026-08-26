import type { Database } from '@delali/sirannon-db'

export interface StatementResult {
  sql: string
  elapsedMs: number
  columns: readonly string[]
  rows: readonly (readonly string[])[]
  rowCount: number
  changes: number | null
  error: string | null
}

export const MAX_DISPLAYED_ROWS = 500

const READ_VERBS = new Set(['select', 'explain', 'pragma', 'values'])
const WRITE_KEYWORD = /\b(insert|update|delete|replace)\b/i
const LEADING_COMMENT = /^(\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/

export function normaliseStatement(sql: string): string {
  return sql.trim().replace(/;\s*$/, '').trim()
}

export function firstVerb(sql: string): string {
  const body = sql.replace(LEADING_COMMENT, '')
  const match = /^[a-zA-Z]+/.exec(body)
  return match === null ? '' : match[0].toLowerCase()
}

export function isReadStatement(sql: string): boolean {
  const verb = firstVerb(sql)
  if (verb === 'with') return !WRITE_KEYWORD.test(sql)
  return READ_VERBS.has(verb)
}

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'string') return value
  if (typeof value === 'bigint') return value.toString()
  if (value instanceof Uint8Array) return `BLOB (${value.byteLength} bytes)`
  return String(value)
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function toRows(records: readonly Record<string, unknown>[], columns: readonly string[]): string[][] {
  return records.slice(0, MAX_DISPLAYED_ROWS).map(record => columns.map(column => formatCell(record[column])))
}

export async function runStatement(db: Database, sql: string): Promise<StatementResult> {
  const statement = normaliseStatement(sql)
  const started = performance.now()
  const base = { sql: statement, columns: [], rows: [], rowCount: 0, changes: null, error: null }

  if (statement === '') {
    return { ...base, elapsedMs: 0, error: 'Type a statement to run it.' }
  }

  try {
    if (isReadStatement(statement)) {
      const records = await db.query(statement)
      const first = records[0]
      const columns = first === undefined ? [] : Object.keys(first)
      return {
        ...base,
        elapsedMs: performance.now() - started,
        columns,
        rows: toRows(records, columns),
        rowCount: records.length,
      }
    }

    const result = await db.execute(statement)
    return { ...base, elapsedMs: performance.now() - started, changes: result.changes }
  } catch (error) {
    return { ...base, elapsedMs: performance.now() - started, error: errorMessage(error) }
  }
}
