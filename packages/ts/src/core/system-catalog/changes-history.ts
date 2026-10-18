import type { SQLiteConnection } from '../driver/types.js'
import { CHANGE_LOG_COLUMNS, seqFromColumn } from './changes-table.js'

function originPlaceholders(originCount: number): string {
  return Array.from({ length: originCount }, () => '?').join(', ')
}

/**
 * Names one transaction in a replication group's history by its timestamp and transaction ID, with the sequence of
 * its last change in this node's change log.
 *
 * @internal
 */
export interface HistoryChangeRef {
  seq: bigint
  hlc: string
  txId: string
}

/**
 * Holds one change of a replication group's history as this node's change log stores it, with the row images that
 * undo the change.
 *
 * @internal
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
 * Returns the statement that reads a replication group's history from the change log: every change that a group
 * member stamped, whether this node wrote it or applied it from another member, oldest first.
 *
 * @param tableName - The change log table.
 * @param originCount - The number of group members, which sets the number of member IDs that the statement binds.
 * @returns The statement, which binds the sequence to read after, then each member ID, then the row limit.
 * @internal
 */
export function selectOutboundHistoryChangesSql(tableName: string, originCount: number): string {
  return `SELECT ${CHANGE_LOG_COLUMNS} FROM "${tableName}"
          WHERE seq > ? AND hlc != '' AND node_id IN (${originPlaceholders(originCount)})
          ORDER BY seq ASC LIMIT ?`
}

/**
 * Returns the history transaction with the latest timestamp in the change log.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param origins - The IDs of the group members whose changes form the history.
 * @param bounds - `maxSeq` keeps only the changes at or below one sequence, and `beforeHlc` keeps only the changes
 * stamped earlier than one timestamp.
 * @returns That transaction, or `null` when the log holds no history change from those members within the bounds.
 * @internal
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
      seq: seqFromColumn(row.seq),
      hlc: row.hlc,
      txId: row.tx_id,
    }
  )
}

/**
 * Returns the highest sequence among the history changes in the change log.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param origins - The IDs of the group members whose changes form the history.
 * @returns That sequence, or zero when the log holds no history change from those members.
 * @internal
 */
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
  return seqFromColumn(row?.seq)
}

/**
 * Returns one history transaction as the change log holds it, with the sequence of its last change and its latest
 * timestamp.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param txId - The ID of the transaction.
 * @returns The transaction, or `null` when the log holds none of its stamped changes.
 * @internal
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
  return { seq: seqFromColumn(row.seq), hlc: row.hlc, txId }
}

/**
 * Returns every history change stamped later than one timestamp, newest sequence first, which is the order in which
 * a node undoes them.
 *
 * @param conn - The connection to read the change log through.
 * @param tableName - The change log table.
 * @param origins - The IDs of the group members whose changes form the history.
 * @param hlc - The timestamp after which to read.
 * @returns The changes with the row images that undo each one, or none when `origins` is empty.
 * @internal
 */
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

/**
 * Deletes every history change stamped later than one timestamp.
 *
 * @param conn - The connection to write the change log through.
 * @param tableName - The change log table.
 * @param origins - The IDs of the group members whose changes form the history.
 * @param hlc - The timestamp after which to delete.
 * @returns A promise that settles once SQLite has deleted the changes, at once when `origins` is empty.
 * @internal
 */
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

/**
 * Deletes every stamped change at or below one sequence, and keeps the unstamped changes.
 *
 * @param conn - The connection to write the change log through.
 * @param tableName - The change log table.
 * @param seq - The highest sequence to delete.
 * @returns A promise that settles once SQLite has deleted the changes.
 * @internal
 */
export async function deleteStampedChangesUpToSeq(
  conn: SQLiteConnection,
  tableName: string,
  seq: bigint,
): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${tableName}" WHERE seq <= ? AND hlc != ''`)
  await stmt.run(seq.toString())
}
