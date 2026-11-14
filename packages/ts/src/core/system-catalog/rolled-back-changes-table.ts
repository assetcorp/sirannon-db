import type { SQLiteConnection } from '../driver/types.js'
import { ROLLED_BACK_CHANGES_TABLE } from '../internal-tables.js'

/**
 * Describes one change that this node undid when it reconciled its history with a new primary's, which the node keeps
 * so that an operator can review a write that the group never held on a majority of its voters.
 *
 * @public
 */
export interface RolledBackChange {
  /** Holds the change's position in this node's change log before the node undid it. */
  seq: bigint
  /** Holds the time, in seconds since the Unix epoch, at which the node undid the change. */
  rolledBackAt: number
  /** Holds the primary term with whose history the node reconciled when it undid the change. */
  rolledBackInTerm: bigint
  /** Holds the name of the table that the change wrote. */
  tableName: string
  /** Holds the change's operation: `INSERT`, `UPDATE`, or `DELETE`. */
  operation: string
  /** Holds the SQLite row ID of the row that the change wrote. */
  rowId: string
  /** Holds the row as it stood before the change, encoded as the change log encodes it, or null for an insert. */
  oldData: string | null
  /** Holds the row as the change left it, encoded as the change log encodes it, or null for a delete. */
  newData: string | null
  /** Holds the ID of the node that wrote the change. */
  nodeId: string
  /** Holds the ID of the transaction that wrote the change. */
  txId: string
  /** Holds the hybrid logical clock timestamp of the change. */
  hlc: string
}

interface RolledBackChangeRow {
  seq: number | bigint
  rolled_back_at: number
  rolled_back_in_term: string
  table_name: string
  operation: string
  row_id: string
  old_data: string | null
  new_data: string | null
  node_id: string
  tx_id: string
  hlc: string
}

function originPlaceholders(originCount: number): string {
  return Array.from({ length: originCount }, () => '?').join(', ')
}

/**
 * Creates the table that keeps the changes that this node undid when it reconciled its history, where the table is
 * missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureRolledBackChangesTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(
    `CREATE TABLE IF NOT EXISTS "${ROLLED_BACK_CHANGES_TABLE}" (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      seq INTEGER NOT NULL UNIQUE,
      rolled_back_at REAL NOT NULL,
      rolled_back_in_term TEXT NOT NULL,
      table_name TEXT NOT NULL,
      operation TEXT NOT NULL,
      row_id TEXT NOT NULL,
      old_data TEXT,
      new_data TEXT,
      node_id TEXT NOT NULL,
      tx_id TEXT NOT NULL,
      hlc TEXT NOT NULL
    )`,
  )
}

/**
 * Copies every history change stamped later than one timestamp from the change log into the table of rolled-back
 * changes, so that the copies outlast the undo that follows in the same transaction. A change that the table already
 * keeps stays as it is.
 *
 * @param conn - The connection that runs the undo's transaction.
 * @param changesTable - The change log table.
 * @param origins - The IDs of the group members whose changes form the history.
 * @param hlc - The timestamp after which to copy, or null to copy every history change.
 * @param term - The primary term with whose history the node reconciles.
 * @param nowSeconds - The time to record for the undo, in seconds since the Unix epoch.
 * @returns A promise that settles once SQLite has copied the changes, at once when `origins` is empty.
 * @internal
 */
export async function insertRolledBackChangesAfterHlc(
  conn: SQLiteConnection,
  changesTable: string,
  origins: readonly string[],
  hlc: string | null,
  term: bigint,
  nowSeconds: number,
): Promise<void> {
  if (origins.length === 0) return
  const stmt = await conn.prepare(
    `INSERT OR IGNORE INTO "${ROLLED_BACK_CHANGES_TABLE}"
      (seq, rolled_back_at, rolled_back_in_term, table_name, operation, row_id, old_data, new_data, node_id, tx_id, hlc)
     SELECT seq, ?, ?, table_name, operation, row_id, old_data, new_data, node_id, tx_id, hlc FROM "${changesTable}"
     WHERE hlc != '' AND hlc > ? AND node_id IN (${originPlaceholders(origins.length)})
     ORDER BY seq`,
  )
  await stmt.run(nowSeconds, term.toString(), hlc ?? '', ...origins)
}

/**
 * Returns every change that this node has rolled back and still keeps, oldest first.
 *
 * @param conn - The connection to read the table through.
 * @returns The rolled-back changes.
 * @internal
 */
export async function selectRolledBackChanges(conn: SQLiteConnection): Promise<RolledBackChange[]> {
  const stmt = await conn.prepare(
    `SELECT seq, rolled_back_at, rolled_back_in_term, table_name, operation, row_id, old_data, new_data, node_id, tx_id, hlc
     FROM "${ROLLED_BACK_CHANGES_TABLE}" ORDER BY id`,
  )
  const rows = (await stmt.all()) as RolledBackChangeRow[]
  return rows.map(row => ({
    seq: BigInt(row.seq),
    rolledBackAt: Number(row.rolled_back_at),
    rolledBackInTerm: BigInt(row.rolled_back_in_term),
    tableName: row.table_name,
    operation: row.operation,
    rowId: row.row_id,
    oldData: row.old_data,
    newData: row.new_data,
    nodeId: row.node_id,
    txId: row.tx_id,
    hlc: row.hlc,
  }))
}

/**
 * Deletes every change that this node keeps from its rollbacks.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once SQLite has deleted the rows.
 * @internal
 */
export async function deleteRolledBackChanges(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM "${ROLLED_BACK_CHANGES_TABLE}"`)
}
