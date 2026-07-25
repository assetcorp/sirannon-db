import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'
import { ensureMetaTable } from './meta-table.js'

const CHANGE_LOG_COLUMNS = 'seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc'

const DDL_CHANGE_ROW_TABLE = '__ddl__'

export function selectChangesAfterSeqSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}" WHERE seq > ? ORDER BY seq ASC LIMIT ?`
}

export function selectTableChangesInRangeSql(tableName: string): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE table_name = ? AND seq > ? AND seq <= ?
          ORDER BY seq ASC LIMIT ?`
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

export async function countOutboundChanges(
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

export async function recordDdlChange(
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

export function stampChangesAfterSeqSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`
}

export function stampUnstampedChangesSql(tableName: string): string {
  return `UPDATE "${tableName}" SET node_id = ?, tx_id = ?, hlc = ? WHERE node_id = ''`
}

export function minChangeSeqSql(tableName: string): string {
  return `SELECT MIN(seq) AS seq FROM "${tableName}"`
}

export function nextForeignChangeSeqSql(tableName: string): string {
  return `SELECT MIN(seq) AS seq FROM "${tableName}" WHERE seq > ? AND node_id != ?`
}

export function deleteChangesBeforeSql(tableName: string): string {
  return `DELETE FROM "${tableName}" WHERE changed_at < ?`
}

export function deleteChangesBeforeUpToSeqSql(tableName: string): string {
  return `DELETE FROM "${tableName}" WHERE changed_at < ? AND seq <= ?`
}

export async function maxRowChangeHlc(
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

export async function maxChangeSeq(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}"`)
  const row = (await stmt.get()) as { seq?: unknown } | undefined
  const seq = row?.seq
  if (seq === undefined || seq === null) return 0n
  return typeof seq === 'bigint' ? seq : BigInt(String(seq))
}

export async function maxChangeHlc(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS hlc FROM "${tableName}" WHERE hlc != ''`)
  const row = (await stmt.get()) as { hlc?: unknown } | undefined
  return typeof row?.hlc === 'string' && row.hlc.length > 0 ? row.hlc : null
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
