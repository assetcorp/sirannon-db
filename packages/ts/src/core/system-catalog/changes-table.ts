import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE, META_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'
import { ensureMetaTable } from './meta-table.js'
import { selectTableExists } from './sqlite-catalog.js'

const SQLITE_SEQUENCE_TABLE = 'sqlite_sequence'

const DDL_CHANGE_ROW_TABLE = '__ddl__'

/**
 * The columns that every read of whole changes takes from the change log, in the order that a change row holds them.
 *
 * @internal
 */
export const CHANGE_LOG_COLUMNS =
  'seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc'

/**
 * Returns a change sequence as SQLite returned it from a column, as a `bigint`.
 *
 * @param value - The column value, which a driver returns as a number, a `bigint`, or a string.
 * @returns The sequence, or zero for `null` and `undefined`.
 * @internal
 */
export function seqFromColumn(value: unknown): bigint {
  if (value === undefined || value === null) return 0n
  return typeof value === 'bigint' ? value : BigInt(String(value))
}

/**
 * Returns the statement that reads the changes after a sequence, oldest first.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds the sequence to read after, then the row limit.
 * @internal
 */
export function selectChangesAfterSeqSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}" WHERE seq > ? ORDER BY seq ASC LIMIT ?`
}

/**
 * Returns the statement that reads the changes to a set of tables within a range of sequences, oldest first.
 *
 * @param tableName - The change log table.
 * @param tableCount - The number of tables, which sets the number of table names that the statement binds.
 * @returns The statement, which binds each table name, then the sequence to read after, then the highest sequence to
 * read, then the row limit.
 * @internal
 */
export function selectTablesChangesInRangeSql(tableName: string, tableCount: number): string {
  const placeholders = Array.from({ length: tableCount }, () => '?').join(', ')
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE table_name IN (${placeholders}) AND seq > ? AND seq <= ?
          ORDER BY seq ASC LIMIT ?`
}

/**
 * Returns the statement that reads every change that one node stamped after a sequence, oldest first.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds the sequence to read after, then the node ID.
 * @internal
 */
export function selectNodeChangesAfterSeqSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND node_id = ?
          ORDER BY seq ASC`
}

/**
 * Returns the statement that reads the changes that one node stamped after a sequence, oldest first, for that node
 * to send to another.
 *
 * @param tableName - The change log table.
 * @param excludeDdl - `true` to leave schema changes out.
 * @returns The statement, which binds the sequence to read after, then the node ID, then the row limit.
 * @internal
 */
export function selectOutboundChangesSql(tableName: string, excludeDdl: boolean): string {
  const ddlFilter = excludeDdl ? "AND operation != 'DDL'" : ''
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND node_id = ? ${ddlFilter}
          ORDER BY seq ASC LIMIT ?`
}

/**
 * Returns the number of row changes that one node stamped after a sequence, leaving out schema changes.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param afterSeq - The sequence to count after.
 * @param nodeId - The node whose changes to count.
 * @returns The number of changes.
 * @internal
 */
export async function selectCountOutboundChanges(
  conn: SQLiteConnection,
  tableName: string,
  afterSeq: bigint,
  nodeId: string,
): Promise<number> {
  const stmt = await conn.prepare(
    `SELECT COUNT(*) AS pending FROM "${tableName}" WHERE seq > ? AND node_id = ? AND operation != 'DDL'`,
  )
  const row = (await stmt.get(afterSeq.toString(), nodeId)) as { pending: number | bigint }
  return Number(row.pending)
}

/**
 * Records a schema change in the change log, stamped with the node, transaction, and timestamp that made it.
 *
 * @param conn - The connection to write the change log through.
 * @param tableName - The change log table.
 * @param entry - The schema change statement and its stamps.
 * @returns A promise that settles once SQLite has inserted the change.
 * @internal
 */
export async function insertDdlChange(
  conn: SQLiteConnection,
  tableName: string,
  entry: { ddlStatement: string; nodeId: string; txId: string; hlc: string },
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO "${tableName}" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
     VALUES ('${DDL_CHANGE_ROW_TABLE}', 'DDL', '', ?, ?, ?, ?)`,
  )
  await stmt.run(JSON.stringify({ ddlStatement: entry.ddlStatement }), entry.nodeId, entry.txId, entry.hlc)
}

/**
 * Returns the statement that stamps each unstamped change above a sequence with a node ID, a transaction ID, and a
 * timestamp.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds the node ID, the transaction ID, the timestamp, then the sequence.
 * @internal
 */
export function updateChangeStampsAfterSeqSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`
}

/**
 * Returns the statement that stamps each unstamped change above the sequence that the meta table holds under one key,
 * with a node ID, a transaction ID, and a timestamp.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds the node ID, the transaction ID, the timestamp, then the meta key, and which
 * stamps every unstamped change when the meta table holds no value under that key.
 * @internal
 */
export function updateChangeStampsAboveMetaSeqSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ?
     WHERE node_id = '' AND seq > COALESCE((SELECT CAST(value AS INTEGER) FROM "${META_TABLE}" WHERE key = ?), 0)`
}

/**
 * Returns the statement that reads the lowest sequence in the change log.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds nothing and returns `seq` as `null` when the log is empty.
 * @internal
 */
export function selectMinChangeSeqSql(tableName: string): string {
  return `SELECT MIN(seq) AS seq FROM "${tableName}"`
}

/**
 * Returns the statement that deletes the changes recorded before a time, at or below a sequence.
 *
 * @param tableName - The change log table.
 * @returns The statement, which binds the time in seconds since the Unix epoch, then the highest sequence to delete.
 * @internal
 */
export function deleteChangesBeforeUpToSeqSql(tableName: string): string {
  return `DELETE FROM "${tableName}" WHERE changed_at < ? AND seq <= ?`
}

/**
 * Returns the latest timestamp among the stamped changes to one row.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param rowTable - The table that holds the row.
 * @param rowId - The row ID that the change log stores for the row.
 * @returns That timestamp, or `null` when the log holds no stamped change to the row.
 * @internal
 */
export async function selectMaxRowChangeHlc(
  conn: SQLiteConnection,
  tableName: string,
  rowTable: string,
  rowId: string,
): Promise<string | null> {
  const stmt = await conn.prepare(
    `SELECT MAX(hlc) AS max_hlc FROM "${tableName}" WHERE table_name = ? AND row_id = ? AND hlc != ''`,
  )
  const row = (await stmt.get(rowTable, rowId)) as { max_hlc?: string | null } | undefined
  return row?.max_hlc ?? null
}

/**
 * Returns the highest sequence that the change log holds.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @returns That sequence, or zero when the log is empty.
 * @internal
 */
export async function selectMaxChangeSeq(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}"`)
  const row = (await stmt.get()) as { seq?: unknown } | undefined
  return seqFromColumn(row?.seq)
}

/**
 * Returns the highest sequence that the change log has ever assigned, which stays put when pruning deletes every row.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @returns That sequence, or zero when the log has never held a change.
 * @internal
 */
export async function selectMaxAssignedChangeSeq(
  conn: SQLiteConnection,
  tableName: string = CHANGES_TABLE,
): Promise<bigint> {
  const logged = await selectMaxChangeSeq(conn, tableName)
  if (!(await selectTableExists(conn, SQLITE_SEQUENCE_TABLE))) return logged
  const stmt = await conn.prepare(`SELECT seq FROM ${SQLITE_SEQUENCE_TABLE} WHERE name = ?`)
  const row = (await stmt.get(tableName)) as { seq?: unknown } | undefined
  const assigned = seqFromColumn(row?.seq)
  return assigned > logged ? assigned : logged
}

/**
 * Returns the highest sequence among the changes that one node made itself, leaving out the changes that it applied
 * from another node.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param nodeId - The node whose own changes to consider.
 * @returns That sequence, or zero when the log holds no change from that node.
 * @internal
 */
export async function selectMaxNodeChangeSeq(
  conn: SQLiteConnection,
  tableName: string,
  nodeId: string,
): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}" WHERE node_id = ?`)
  const row = (await stmt.get(nodeId)) as { seq?: unknown } | undefined
  return seqFromColumn(row?.seq)
}

/**
 * Returns the latest timestamp among the stamped changes in the change log.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @returns That timestamp, or `null` when the log holds no stamped change.
 * @internal
 */
export async function selectMaxChangeHlc(
  conn: SQLiteConnection,
  tableName: string = CHANGES_TABLE,
): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS hlc FROM "${tableName}" WHERE hlc != ''`)
  const row = (await stmt.get()) as { hlc?: unknown } | undefined
  return typeof row?.hlc === 'string' && row.hlc.length > 0 ? row.hlc : null
}

/**
 * Creates the change log and its indexes where they are missing, adds the stamp columns to a log that an earlier
 * version created without them, and creates the meta table.
 *
 * @param conn - The writer connection.
 * @param tableName - The change log table, which has to be a safe identifier.
 * @returns A promise that settles once the change log and the meta table exist.
 * @internal
 */
export async function ensureChangesTable(conn: SQLiteConnection, tableName: string): Promise<void> {
  assertSafeIdentifier(tableName)

  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${tableName}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT,
  node_id TEXT NOT NULL DEFAULT '',
  tx_id TEXT NOT NULL DEFAULT '',
  hlc TEXT NOT NULL DEFAULT ''
)`)

  await ensureColumn(conn, tableName, 'node_id', 'TEXT', '')
  await ensureColumn(conn, tableName, 'tx_id', 'TEXT', '')
  await ensureColumn(conn, tableName, 'hlc', 'TEXT', '')

  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_changed_at" ON "${tableName}" (changed_at)`)
  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_node_id" ON "${tableName}" (node_id)`)
  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_hlc" ON "${tableName}" (hlc)`)

  await ensureMetaTable(conn)
}
