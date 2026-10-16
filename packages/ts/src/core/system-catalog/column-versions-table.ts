import type { SQLiteConnection } from '../driver/types.js'
import { COLUMN_VERSIONS_TABLE } from '../internal-tables.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

const UPSERT_COLUMN_VERSION = `INSERT INTO ${COLUMN_VERSIONS_TABLE} (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name, row_id, column_name)
       DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`

/**
 * Creates the table that holds, for each column of each row, the timestamp and the node of its last write, where
 * the table is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureColumnVersionsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${COLUMN_VERSIONS_TABLE}" (
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  column_name TEXT NOT NULL,
  hlc TEXT NOT NULL,
  node_id TEXT NOT NULL,
  PRIMARY KEY (table_name, row_id, column_name)
)`)
}

/**
 * Returns the latest timestamp that the table holds across every column.
 *
 * @param conn - The connection to read the table through.
 * @returns That timestamp, or `null` when the table holds no stamped column.
 * @internal
 */
export async function selectMaxColumnVersionHlc(conn: SQLiteConnection): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS max_hlc FROM ${COLUMN_VERSIONS_TABLE} WHERE hlc != ''`)
  const row = (await stmt.get()) as { max_hlc?: string | null } | undefined
  return row?.max_hlc ?? null
}

/**
 * Returns the latest timestamp that the table holds across the columns of one row.
 *
 * @param conn - The connection to read the table through.
 * @param tableName - The row's table.
 * @param rowId - The row's ID, as the change log records it.
 * @returns That timestamp, or `null` when the table holds nothing for the row.
 * @internal
 */
export async function selectMaxColumnVersionHlcForRow(
  conn: SQLiteConnection,
  tableName: string,
  rowId: string,
): Promise<string | null> {
  const stmt = await conn.prepare(
    `SELECT MAX(hlc) AS max_hlc FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`,
  )
  const row = (await stmt.get(tableName, rowId)) as { max_hlc?: string | null } | undefined
  return row?.max_hlc ?? null
}

/**
 * Deletes every column version stamped later than one timestamp.
 *
 * @param conn - The writer connection.
 * @param hlc - The timestamp.
 * @returns A promise that settles once SQLite has deleted the rows.
 * @internal
 */
export async function deleteColumnVersionsAfterHlc(conn: SQLiteConnection, hlc: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE hlc > ?`)
  await stmt.run(hlc)
}

/**
 * Deletes every column version.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once SQLite has deleted the rows.
 * @internal
 */
export async function deleteColumnVersions(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM ${COLUMN_VERSIONS_TABLE}`)
}

/**
 * Returns a prepared statement that records a column's last write and replaces the version already there.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the table name, the row ID, the column name, the timestamp, then the node ID.
 * @internal
 */
export function prepareUpsertColumnVersion(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(UPSERT_COLUMN_VERSION)
}

/**
 * Returns a prepared statement that records a column's last write, and replaces the version already there only
 * when the new timestamp is later.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the table name, the row ID, the column name, the timestamp, then the node ID.
 * @internal
 */
export function prepareUpsertNewerColumnVersion(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`${UPSERT_COLUMN_VERSION}
       WHERE excluded.hlc > ${COLUMN_VERSIONS_TABLE}.hlc`)
}

/**
 * Returns a prepared statement that deletes the version of every column of one row.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the table name, then the row ID.
 * @internal
 */
export function prepareDeleteRowColumnVersions(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`)
}

/**
 * Returns a prepared statement that deletes the versions of one row's columns stamped at or before a timestamp.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the table name, the row ID, then the timestamp.
 * @internal
 */
export function prepareDeleteRowColumnVersionsUpToHlc(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ? AND hlc <= ?`)
}
