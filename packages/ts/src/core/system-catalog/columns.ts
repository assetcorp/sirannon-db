import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'

const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const SAFE_COLUMN_TYPE = /^[A-Za-z]+$/
const SAFE_DEFAULT_TEXT = /^[A-Za-z0-9_]*$/

interface ColumnInfoRow {
  name: string
}

export function assertSafeIdentifier(value: string): void {
  if (!SAFE_IDENTIFIER.test(value)) {
    throw new SirannonError(`Unsafe SQL identifier for internal schema: ${value}`, 'INTERNAL_SCHEMA_ERROR')
  }
}

export async function tableColumns(conn: SQLiteConnection, table: string): Promise<Set<string>> {
  assertSafeIdentifier(table)
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const info = (await stmt.all()) as ColumnInfoRow[]
  return new Set(info.map(col => col.name))
}

export async function ensureColumn(
  conn: SQLiteConnection,
  table: string,
  column: string,
  columnType: string,
  notNullDefaultText?: string,
): Promise<void> {
  assertSafeIdentifier(table)
  assertSafeIdentifier(column)
  if (!SAFE_COLUMN_TYPE.test(columnType)) {
    throw new SirannonError(`Unsafe column type for internal schema: ${columnType}`, 'INTERNAL_SCHEMA_ERROR')
  }
  if (notNullDefaultText !== undefined && !SAFE_DEFAULT_TEXT.test(notNullDefaultText)) {
    throw new SirannonError(`Unsafe column default for internal schema: ${notNullDefaultText}`, 'INTERNAL_SCHEMA_ERROR')
  }

  if ((await tableColumns(conn, table)).has(column)) {
    return
  }

  const constraint = notNullDefaultText === undefined ? '' : ` NOT NULL DEFAULT '${notNullDefaultText}'`

  try {
    await conn.exec(`ALTER TABLE "${table}" ADD COLUMN "${column}" ${columnType}${constraint}`)
  } catch (err) {
    if ((await tableColumns(conn, table)).has(column)) {
      return
    }
    throw err
  }
}
