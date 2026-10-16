import type { SQLiteConnection } from '../driver/types.js'
import { STAGED_PUSH_CHANGES_TABLE } from '../internal-tables.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

type SeqValue = number | bigint | string | null | undefined

/**
 * Holds one change that the server keeps for a transaction which a device has not finished pushing, as JSON.
 *
 * @internal
 */
export interface StagedPushChangeRow {
  source_seq: SeqValue
  change: string
}

/**
 * Names the transaction that the server keeps for a device, with the highest sequence that it holds of it.
 *
 * @internal
 */
export interface StagedPushTail {
  txId: string
  maxSeq: bigint
}

/**
 * Creates the table that holds the changes of a transaction which a device has not finished pushing, where the table
 * is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureStagedPushChangesTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${STAGED_PUSH_CHANGES_TABLE}" (
  source_node_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  tx_id TEXT NOT NULL,
  change TEXT NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
)`)
}

/**
 * Returns a prepared statement that holds one pushed change, and leaves a change that the table already holds
 * unchanged.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the device ID, the change's sequence on the device, its transaction ID, then
 * the change as JSON.
 * @internal
 */
export function prepareInsertStagedPushChange(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(
    `INSERT OR IGNORE INTO "${STAGED_PUSH_CHANGES_TABLE}" (source_node_id, source_seq, tx_id, change) VALUES (?, ?, ?, ?)`,
  )
}

/**
 * Returns the transaction that the table holds for one device, with the highest sequence that it holds of it.
 *
 * @param conn - The connection to read the table through.
 * @param sourceNodeId - The device.
 * @returns The transaction, or `null` when the table holds nothing for the device.
 * @internal
 */
export async function selectStagedPushTail(
  conn: SQLiteConnection,
  sourceNodeId: string,
): Promise<StagedPushTail | null> {
  const stmt = await conn.prepare(
    `SELECT tx_id, source_seq FROM "${STAGED_PUSH_CHANGES_TABLE}"
     WHERE source_node_id = ? ORDER BY source_seq DESC LIMIT 1`,
  )
  const row = (await stmt.get(sourceNodeId)) as { tx_id: string; source_seq: SeqValue } | undefined
  if (row === undefined || row.source_seq === null || row.source_seq === undefined) return null
  return { txId: row.tx_id, maxSeq: BigInt(String(row.source_seq)) }
}

/**
 * Returns the sequences that the table holds for one device within a range.
 *
 * @param conn - The connection to read the table through.
 * @param sourceNodeId - The device.
 * @param fromSeq - The lowest sequence to return.
 * @param toSeq - The highest sequence to return.
 * @returns The held sequences in that range, each as a decimal string.
 * @internal
 */
export async function selectStagedPushSeqsInRange(
  conn: SQLiteConnection,
  sourceNodeId: string,
  fromSeq: bigint,
  toSeq: bigint,
): Promise<Set<string>> {
  const stmt = await conn.prepare(
    `SELECT source_seq FROM "${STAGED_PUSH_CHANGES_TABLE}"
     WHERE source_node_id = ? AND source_seq >= ? AND source_seq <= ?`,
  )
  const rows = (await stmt.all(sourceNodeId, fromSeq.toString(), toSeq.toString())) as Array<{
    source_seq: SeqValue
  }>
  return new Set(rows.map(row => String(row.source_seq)))
}

/**
 * Returns the changes that the table holds for one device after a sequence, oldest first.
 *
 * @param conn - The connection to read the table through.
 * @param sourceNodeId - The device.
 * @param afterSeq - The sequence to read after.
 * @param limit - The highest number of changes to return.
 * @returns The held changes, each as JSON with its sequence.
 * @internal
 */
export async function selectStagedPushChangesAfter(
  conn: SQLiteConnection,
  sourceNodeId: string,
  afterSeq: bigint,
  limit: number,
): Promise<StagedPushChangeRow[]> {
  const stmt = await conn.prepare(
    `SELECT source_seq, change FROM "${STAGED_PUSH_CHANGES_TABLE}"
     WHERE source_node_id = ? AND source_seq > ? ORDER BY source_seq ASC LIMIT ?`,
  )
  return (await stmt.all(sourceNodeId, afterSeq.toString(), limit)) as StagedPushChangeRow[]
}

/**
 * Deletes every change that the table holds for one device.
 *
 * @param conn - The writer connection.
 * @param sourceNodeId - The device.
 * @returns A promise that settles once SQLite has deleted the changes.
 * @internal
 */
export async function deleteStagedPushChanges(conn: SQLiteConnection, sourceNodeId: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${STAGED_PUSH_CHANGES_TABLE}" WHERE source_node_id = ?`)
  await stmt.run(sourceNodeId)
}
