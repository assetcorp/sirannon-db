import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'

export async function ensureMetaTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`CREATE TABLE IF NOT EXISTS "${META_TABLE}" (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
}

export async function selectMetaValue(conn: SQLiteConnection, key: string): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT value FROM "${META_TABLE}" WHERE key = ?`)
  const row = (await stmt.get(key)) as { value?: unknown } | undefined
  const value = row?.value
  return typeof value === 'string' ? value : null
}

export async function insertMetaValueIfAbsent(conn: SQLiteConnection, key: string, value: string): Promise<void> {
  const stmt = await conn.prepare(`INSERT OR IGNORE INTO "${META_TABLE}" (key, value) VALUES (?, ?)`)
  await stmt.run(key, value)
}

export async function deleteMetaValue(conn: SQLiteConnection, key: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${META_TABLE}" WHERE key = ?`)
  await stmt.run(key)
}

export const UPSERT_META_VALUE_SQL = `INSERT INTO "${META_TABLE}" (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`

export async function upsertMetaValue(conn: SQLiteConnection, key: string, value: string): Promise<void> {
  const stmt = await conn.prepare(UPSERT_META_VALUE_SQL)
  await stmt.run(key, value)
}
