import type { SQLiteConnection } from '../driver/types.js'
import { APPLIED_CHANGES_TABLE, STAGED_PUSH_CHANGES_TABLE } from '../internal-tables.js'
import { seqFromColumn } from './changes-table.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

type SeqValue = number | bigint | string | null | undefined

/**
 * Creates the table that records each change applied from another node or device, keyed by its source and its
 * sequence there, where the table is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureAppliedChangesTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${APPLIED_CHANGES_TABLE}" (
  source_node_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  applied_at REAL NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
)`)
}

/**
 * Returns a prepared statement that records one applied change, and leaves a record that already exists unchanged.
 *
 * @param conn - The writer connection.
 * @returns The statement, which binds the source node ID, the source sequence, then the time of the apply in seconds
 * since the Unix epoch.
 * @internal
 */
export function prepareInsertAppliedChange(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(
    `INSERT OR IGNORE INTO ${APPLIED_CHANGES_TABLE} (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)`,
  )
}

/**
 * Records as applied every change that the staged-push table holds for one source device, in the transaction that
 * applies them.
 *
 * @param conn - The writer connection, inside the transaction that applies the staged changes.
 * @param sourceNodeId - The device whose staged changes the transaction applies.
 * @param appliedAt - The time of the apply, in seconds since the Unix epoch.
 * @returns A promise that settles once SQLite has inserted the records.
 * @internal
 */
export async function insertAppliedChangesFromStagedPush(
  conn: SQLiteConnection,
  sourceNodeId: string,
  appliedAt: number,
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT OR IGNORE INTO "${APPLIED_CHANGES_TABLE}" (source_node_id, source_seq, applied_at)
     SELECT source_node_id, source_seq, ? FROM "${STAGED_PUSH_CHANGES_TABLE}" WHERE source_node_id = ?`,
  )
  await stmt.run(appliedAt, sourceNodeId)
}

/**
 * Deletes every applied-change record of one source above one sequence.
 *
 * @param conn - The writer connection.
 * @param sourceNodeId - The source whose records to delete.
 * @param seq - The sequence above which to delete.
 * @returns A promise that settles once SQLite has deleted the records.
 * @internal
 */
export async function deleteAppliedChangesAfterSeq(
  conn: SQLiteConnection,
  sourceNodeId: string,
  seq: bigint,
): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ? AND source_seq > ?`)
  await stmt.run(sourceNodeId, seq.toString())
}

/**
 * Returns the highest sequence applied from one source.
 *
 * @param conn - The connection to read the table through.
 * @param sourceNodeId - The source node or device.
 * @returns That sequence, or zero when nothing from the source has been applied.
 * @internal
 */
export async function selectMaxAppliedSourceSeq(conn: SQLiteConnection, sourceNodeId: string): Promise<bigint> {
  const stmt = await conn.prepare(
    `SELECT MAX(source_seq) AS max_seq FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ?`,
  )
  const row = (await stmt.get(sourceNodeId)) as { max_seq?: SeqValue } | undefined
  return seqFromColumn(row?.max_seq)
}

/**
 * Returns the highest sequence applied from each source.
 *
 * @param conn - The connection to read the table through.
 * @returns One entry per source node or device that has an applied change, keyed by its ID.
 * @internal
 */
export async function selectMaxAppliedSourceSeqByNode(conn: SQLiteConnection): Promise<Map<string, bigint>> {
  const stmt = await conn.prepare(
    `SELECT source_node_id, MAX(source_seq) AS max_seq FROM ${APPLIED_CHANGES_TABLE} GROUP BY source_node_id`,
  )
  const rows = (await stmt.all()) as Array<{ source_node_id: string; max_seq: SeqValue }>
  const byNode = new Map<string, bigint>()
  for (const row of rows) {
    if (row.max_seq === null || row.max_seq === undefined) continue
    byNode.set(row.source_node_id, seqFromColumn(row.max_seq))
  }
  return byNode
}

/**
 * Returns the sequences applied from one source within a range.
 *
 * @param conn - The connection to read the table through.
 * @param sourceNodeId - The source node or device.
 * @param fromSeq - The lowest sequence to return.
 * @param toSeqBound - The highest sequence to return.
 * @returns The applied sequences in that range, each as a decimal string.
 * @internal
 */
export async function selectAppliedSourceSeqsInRange(
  conn: SQLiteConnection,
  sourceNodeId: string,
  fromSeq: bigint,
  toSeqBound: bigint,
): Promise<Set<string>> {
  const stmt = await conn.prepare(
    `SELECT source_seq FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ? AND source_seq >= ? AND source_seq <= ?`,
  )
  const rows = (await stmt.all(sourceNodeId, fromSeq.toString(), toSeqBound.toString())) as Array<{
    source_seq: SeqValue
  }>
  const seqs = new Set<string>()
  for (const row of rows) {
    seqs.add(String(row.source_seq))
  }
  return seqs
}
