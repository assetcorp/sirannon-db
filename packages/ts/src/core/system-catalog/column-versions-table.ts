import type { SQLiteConnection } from '../driver/types.js'
import { COLUMN_VERSIONS_TABLE } from '../internal-tables.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

const UPSERT_COLUMN_VERSION = `INSERT INTO ${COLUMN_VERSIONS_TABLE} (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name, row_id, column_name)
       DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`

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

export async function maxColumnVersionHlc(conn: SQLiteConnection): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS max_hlc FROM ${COLUMN_VERSIONS_TABLE} WHERE hlc != ''`)
  const row = (await stmt.get()) as { max_hlc?: string | null } | undefined
  return row?.max_hlc ?? null
}

export async function maxColumnVersionHlcForRow(
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

export function prepareColumnVersionUpsert(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(UPSERT_COLUMN_VERSION)
}

export function prepareNewerColumnVersionUpsert(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`${UPSERT_COLUMN_VERSION}
       WHERE excluded.hlc > ${COLUMN_VERSIONS_TABLE}.hlc`)
}

export function prepareColumnVersionRowDelete(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`)
}

export function prepareColumnVersionRowDeleteUpToHlc(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ? AND hlc <= ?`)
}
