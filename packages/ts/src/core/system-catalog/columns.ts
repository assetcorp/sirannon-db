import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'

const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const SAFE_COLUMN_TYPE = /^[A-Za-z]+$/

interface ColumnInfoRow {
  name: string
}

function assertSafeIdentifier(value: string): void {
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
): Promise<void> {
  assertSafeIdentifier(table)
  assertSafeIdentifier(column)
  if (!SAFE_COLUMN_TYPE.test(columnType)) {
    throw new SirannonError(`Unsafe column type for internal schema: ${columnType}`, 'INTERNAL_SCHEMA_ERROR')
  }

  if ((await tableColumns(conn, table)).has(column)) {
    return
  }

  try {
    await conn.exec(`ALTER TABLE "${table}" ADD COLUMN "${column}" ${columnType}`)
  } catch (err) {
    if ((await tableColumns(conn, table)).has(column)) {
      return
    }
    throw err
  }
}
