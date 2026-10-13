import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE, META_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'
import { ensureMetaTable } from './meta-table.js'
import { selectTableExists } from './sqlite-catalog.js'

const SQLITE_SEQUENCE_TABLE = 'sqlite_sequence'

const CHANGE_LOG_COLUMNS = 'seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc'

const DDL_CHANGE_ROW_TABLE = '__ddl__'

export function selectChangesAfterSeqSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}" WHERE seq > ? ORDER BY seq ASC LIMIT ?`
}

export function selectTablesChangesInRangeSql(tableName: string, tableCount: number): string {
  const placeholders = Array.from({ length: tableCount }, () => '?').join(', ')
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE table_name IN (${placeholders}) AND seq > ? AND seq <= ?
          ORDER BY seq ASC LIMIT ?`
}

export function selectNodeChangesAfterSeqSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND node_id = ?
          ORDER BY seq ASC`
}

export function selectOutboundChangesSql(tableName: string, excludeDdl: boolean): string {
  const ddlFilter = excludeDdl ? "AND operation != 'DDL'" : ''
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND node_id = ? ${ddlFilter}
          ORDER BY seq ASC LIMIT ?`
}

function originPlaceholders(originCount: number): string {
  return Array.from({ length: originCount }, () => '?').join(', ')
}

/**
 * Returns the statement that reads a replication group's history from the change log: every change that a group
 * member stamped, whether this node wrote it or applied it from another member. It binds the sequence to read after,
 * then each member ID, then the row limit.
 */
export function selectOutboundHistoryChangesSql(tableName: string, originCount: number): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND hlc != '' AND node_id IN (${originPlaceholders(originCount)})
          ORDER BY seq ASC LIMIT ?`
}

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

export function updateChangeStampsAfterSeqSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`
}

export function updateChangeStampsAboveMetaSeqSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ?
     WHERE node_id = '' AND seq > COALESCE((SELECT CAST(value AS INTEGER) FROM "${META_TABLE}" WHERE key = ?), 0)`
}

export function selectMinChangeSeqSql(tableName: string): string {
  return `SELECT MIN(seq) AS seq FROM "${tableName}"`
}

export function deleteChangesBeforeUpToSeqSql(tableName: string): string {
  return `DELETE FROM "${tableName}" WHERE changed_at < ? AND seq <= ?`
}

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

export async function selectMaxChangeSeq(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}"`)
  const row = (await stmt.get()) as { seq?: unknown } | undefined
  const seq = row?.seq
  if (seq === undefined || seq === null) return 0n
  return typeof seq === 'bigint' ? seq : BigInt(String(seq))
}

/**
 * Returns the highest sequence that the change log has ever assigned, which stays put when pruning deletes every row.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @returns That sequence, or zero when the log has never held a change.
 */
export async function selectMaxAssignedChangeSeq(
  conn: SQLiteConnection,
  tableName: string = CHANGES_TABLE,
): Promise<bigint> {
  const logged = await selectMaxChangeSeq(conn, tableName)
  if (!(await selectTableExists(conn, SQLITE_SEQUENCE_TABLE))) return logged
  const stmt = await conn.prepare(`SELECT seq FROM ${SQLITE_SEQUENCE_TABLE} WHERE name = ?`)
  const row = (await stmt.get(tableName)) as { seq?: unknown } | undefined
  const assigned = seqOf(row?.seq)
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
 */
export async function selectMaxNodeChangeSeq(
  conn: SQLiteConnection,
  tableName: string,
  nodeId: string,
): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}" WHERE node_id = ?`)
  const row = (await stmt.get(nodeId)) as { seq?: unknown } | undefined
  const seq = row?.seq
  if (seq === undefined || seq === null) return 0n
  return typeof seq === 'bigint' ? seq : BigInt(String(seq))
}

export async function selectMaxChangeHlc(
  conn: SQLiteConnection,
  tableName: string = CHANGES_TABLE,
): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS hlc FROM "${tableName}" WHERE hlc != ''`)
  const row = (await stmt.get()) as { hlc?: unknown } | undefined
  return typeof row?.hlc === 'string' && row.hlc.length > 0 ? row.hlc : null
}

function seqOf(value: unknown): bigint {
  if (value === undefined || value === null) return 0n
  return typeof value === 'bigint' ? value : BigInt(String(value))
}

/**
 * Names one transaction in a replication group's history by its timestamp and transaction ID, with the sequence of
 * its last change in this node's change log.
 */
export interface HistoryChangeRef {
  seq: bigint
  hlc: string
  txId: string
}

/**
 * Holds one change of a replication group's history as this node's change log stores it, with the row images that
 * undo the change.
 */
export interface HistoryChangeRow {
  seq: number | bigint
  table_name: string
  operation: string
  row_id: string
  old_data: string | null
  new_data: string | null
}

/**
 * Returns the history transaction with the latest timestamp in the change log, or null when the log holds no history
 * change from the given group members. `maxSeq` keeps only the changes at or below one sequence, and `beforeHlc` keeps
 * only the changes stamped earlier than one timestamp.
 */
export async function selectNewestHistoryChange(
  conn: SQLiteConnection,
  tableName: string,
  origins: readonly string[],
  bounds: { maxSeq?: bigint; beforeHlc?: string } = {},
): Promise<HistoryChangeRef | null> {
  if (origins.length === 0) return null
  const seqFilter = bounds.maxSeq === undefined ? '' : 'AND seq <= ?'
  const hlcFilter = bounds.beforeHlc === undefined ? '' : 'AND hlc < ?'
  const stmt = await conn.prepare(
    `SELECT seq, hlc, tx_id FROM "${tableName}"
     WHERE hlc != '' AND node_id IN (${originPlaceholders(origins.length)}) ${seqFilter} ${hlcFilter}
     ORDER BY hlc DESC, seq DESC LIMIT 1`,
  )
  const params: unknown[] = [...origins]
  if (bounds.maxSeq !== undefined) params.push(bounds.maxSeq.toString())
  if (bounds.beforeHlc !== undefined) params.push(bounds.beforeHlc)
  const row = (await stmt.get(...params)) as { seq: unknown; hlc: string; tx_id: string } | undefined
  if (!row) return null
  return (
    (await selectHistoryTransaction(conn, tableName, row.tx_id)) ?? {
      seq: seqOf(row.seq),
      hlc: row.hlc,
      txId: row.tx_id,
    }
  )
}

/** Returns the highest sequence among the history changes in the change log, or zero when the log holds none. */
export async function selectMaxHistoryChangeSeq(
  conn: SQLiteConnection,
  tableName: string,
  origins: readonly string[],
): Promise<bigint> {
  if (origins.length === 0) return 0n
  const stmt = await conn.prepare(
    `SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}"
     WHERE hlc != '' AND node_id IN (${originPlaceholders(origins.length)})`,
  )
  const row = (await stmt.get(...origins)) as { seq?: unknown } | undefined
  return seqOf(row?.seq)
}

/**
 * Returns one history transaction as the change log holds it, with the sequence of its last change and its latest
 * timestamp, or null when the log holds none of its changes.
 */
export async function selectHistoryTransaction(
  conn: SQLiteConnection,
  tableName: string,
  txId: string,
): Promise<HistoryChangeRef | null> {
  const stmt = await conn.prepare(
    `SELECT MAX(seq) AS seq, MAX(hlc) AS hlc FROM "${tableName}" WHERE tx_id = ? AND hlc != ''`,
  )
  const row = (await stmt.get(txId)) as { seq?: unknown; hlc?: unknown } | undefined
  if (row?.seq === undefined || row.seq === null || typeof row.hlc !== 'string') return null
  return { seq: seqOf(row.seq), hlc: row.hlc, txId }
}

/** Returns every history change stamped later than one timestamp, newest sequence first. */
export async function selectHistoryChangesAfterHlc(
  conn: SQLiteConnection,
  tableName: string,
  origins: readonly string[],
  hlc: string,
): Promise<HistoryChangeRow[]> {
  if (origins.length === 0) return []
  const stmt = await conn.prepare(
    `SELECT seq, table_name, operation, row_id, old_data, new_data FROM "${tableName}"
     WHERE hlc > ? AND node_id IN (${originPlaceholders(origins.length)})
     ORDER BY seq DESC`,
  )
  return (await stmt.all(hlc, ...origins)) as HistoryChangeRow[]
}

/** Deletes every history change stamped later than one timestamp. */
export async function deleteHistoryChangesAfterHlc(
  conn: SQLiteConnection,
  tableName: string,
  origins: readonly string[],
  hlc: string,
): Promise<void> {
  if (origins.length === 0) return
  const stmt = await conn.prepare(
    `DELETE FROM "${tableName}" WHERE hlc > ? AND node_id IN (${originPlaceholders(origins.length)})`,
  )
  await stmt.run(hlc, ...origins)
}

/** Deletes every stamped change at or below one sequence. */
export async function deleteStampedChangesUpToSeq(
  conn: SQLiteConnection,
  tableName: string,
  seq: bigint,
): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${tableName}" WHERE seq <= ? AND hlc != ''`)
  await stmt.run(seq.toString())
}

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
