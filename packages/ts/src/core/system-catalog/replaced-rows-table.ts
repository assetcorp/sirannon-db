import type { SQLiteConnection } from '../driver/types.js'
import { REPLACED_ROWS_TABLE } from '../internal-tables.js'

/**
 * Creates the table that holds each row of a watched table that a pending `REPLACE` is about to delete, and its index by conflict, unless they already exist.
 *
 * @param conn - The writer connection that installs the change capture triggers.
 * @internal
 */
export async function ensureReplacedRowsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(
    `CREATE TABLE IF NOT EXISTS "${REPLACED_ROWS_TABLE}" (
      table_name TEXT NOT NULL,
      row_key NOT NULL,
      match_key TEXT NOT NULL,
      row_id NOT NULL,
      old_data TEXT,
      UNIQUE (table_name, row_key)
    )`,
  )
  await conn.exec(
    `CREATE INDEX IF NOT EXISTS "${REPLACED_ROWS_TABLE}_by_match" ON "${REPLACED_ROWS_TABLE}" (table_name, match_key)`,
  )
}

/**
 * Deletes every row that the change capture triggers hold for one table while a `REPLACE` is pending.
 *
 * @param conn - The writer connection.
 * @param table - The stored name of the table that the rows belong to.
 * @internal
 */
export async function deleteReplacedRowsOfTable(conn: SQLiteConnection, table: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${REPLACED_ROWS_TABLE}" WHERE table_name = ?`)
  await stmt.run(table)
}

/**
 * Deletes every row that the change capture triggers hold, which is safe only between statements, because no `REPLACE` is pending there.
 *
 * @param conn - The writer connection, held outside any statement.
 * @internal
 */
export async function deleteAllReplacedRows(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM "${REPLACED_ROWS_TABLE}"`)
}
