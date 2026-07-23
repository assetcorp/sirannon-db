import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'

export async function ensureMetaTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`CREATE TABLE IF NOT EXISTS "${META_TABLE}" (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
}

export async function getMetaValue(conn: SQLiteConnection, key: string): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT value FROM "${META_TABLE}" WHERE key = ?`)
  const row = (await stmt.get(key)) as { value?: unknown } | undefined
  const value = row?.value
  return typeof value === 'string' ? value : null
}

export async function setMetaValue(conn: SQLiteConnection, key: string, value: string): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO "${META_TABLE}" (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
  )
  await stmt.run(key, value)
}
