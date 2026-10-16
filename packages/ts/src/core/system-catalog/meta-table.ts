import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'

/**
 * Creates the key and value table that holds Sirannon's own settings for a database, where the table is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureMetaTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`CREATE TABLE IF NOT EXISTS "${META_TABLE}" (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
}

/**
 * Returns the value stored under a key.
 *
 * @param conn - The connection to read the table through.
 * @param key - The key.
 * @returns The value, or `null` when the table holds no text value under the key.
 * @internal
 */
export async function selectMetaValue(conn: SQLiteConnection, key: string): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT value FROM "${META_TABLE}" WHERE key = ?`)
  const row = (await stmt.get(key)) as { value?: unknown } | undefined
  const value = row?.value
  return typeof value === 'string' ? value : null
}

/**
 * Stores a value under a key that has none, and leaves a stored value unchanged.
 *
 * @param conn - The writer connection.
 * @param key - The key.
 * @param value - The value to store.
 * @returns A promise that settles once SQLite has written the row or found one in place.
 * @internal
 */
export async function insertMetaValueIfAbsent(conn: SQLiteConnection, key: string, value: string): Promise<void> {
  const stmt = await conn.prepare(`INSERT OR IGNORE INTO "${META_TABLE}" (key, value) VALUES (?, ?)`)
  await stmt.run(key, value)
}

/**
 * Deletes the value stored under a key.
 *
 * @param conn - The writer connection.
 * @param key - The key.
 * @returns A promise that settles once SQLite has deleted the row.
 * @internal
 */
export async function deleteMetaValue(conn: SQLiteConnection, key: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${META_TABLE}" WHERE key = ?`)
  await stmt.run(key)
}

/**
 * Holds the statement that stores a value under a key and replaces any value already there. It binds the key, then
 * the value.
 *
 * @internal
 */
export const UPSERT_META_VALUE_SQL = `INSERT INTO "${META_TABLE}" (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`

/**
 * Returns the statement that stores the change log's highest sequence under a key, as text, and replaces any value
 * already there. The statement binds the key, and it stores zero when the log is empty.
 *
 * @param changesTable - The change log table.
 * @returns The statement text.
 * @internal
 */
export function upsertMetaMaxChangeSeqSql(changesTable: string): string {
  return `INSERT INTO "${META_TABLE}" (key, value)
     VALUES (?, (SELECT CAST(COALESCE(MAX(seq), 0) AS TEXT) FROM "${changesTable}"))
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`
}

/**
 * Returns the statement that stores a value under one key and the change log's highest sequence, as text, under a
 * second key, replacing any value already there, so that one statement records both.
 *
 * The statement binds the first key, its value, and the second key, in that order, and it stores zero for the
 * sequence when the log is empty.
 *
 * @param changesTable - The change log table.
 * @returns The statement text.
 * @internal
 */
export function upsertMetaValueAndMaxChangeSeqSql(changesTable: string): string {
  return `INSERT INTO "${META_TABLE}" (key, value)
     VALUES (?, ?), (?, (SELECT CAST(COALESCE(MAX(seq), 0) AS TEXT) FROM "${changesTable}"))
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`
}

/**
 * Stores a value under a key and replaces any value already there.
 *
 * @param conn - The writer connection.
 * @param key - The key.
 * @param value - The value to store.
 * @returns A promise that settles once SQLite has written the row.
 * @internal
 */
export async function upsertMetaValue(conn: SQLiteConnection, key: string, value: string): Promise<void> {
  const stmt = await conn.prepare(UPSERT_META_VALUE_SQL)
  await stmt.run(key, value)
}
