import type { SQLiteConnection } from '../driver/types.js'
import { APPLIED_CHANGES_TABLE, STAGED_PUSH_CHANGES_TABLE } from '../internal-tables.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

type SeqValue = number | bigint | string | null | undefined

export interface StagedPushChangeRow {
  source_seq: SeqValue
  change: string
}

export interface StagedPushTail {
  txId: string
  maxSeq: bigint
}

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

export function prepareInsertStagedPushChange(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(
    `INSERT OR IGNORE INTO "${STAGED_PUSH_CHANGES_TABLE}" (source_node_id, source_seq, tx_id, change) VALUES (?, ?, ?, ?)`,
  )
}

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

export async function deleteStagedPushChanges(conn: SQLiteConnection, sourceNodeId: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${STAGED_PUSH_CHANGES_TABLE}" WHERE source_node_id = ?`)
  await stmt.run(sourceNodeId)
}
