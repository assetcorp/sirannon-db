import type { SQLiteConnection } from '../driver/types.js'
import { APPLIED_CHANGES_TABLE } from '../internal-tables.js'

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

type SeqValue = number | bigint | string | null | undefined

function toSeq(value: SeqValue): bigint {
  if (value === null || value === undefined) return 0n
  return typeof value === 'bigint' ? value : BigInt(String(value))
}

export async function ensureAppliedChangesTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${APPLIED_CHANGES_TABLE}" (
  source_node_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  applied_at REAL NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
)`)
}

export function prepareAppliedChangesInsert(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(
    `INSERT OR IGNORE INTO ${APPLIED_CHANGES_TABLE} (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)`,
  )
}

export async function maxAppliedSourceSeq(conn: SQLiteConnection, sourceNodeId: string): Promise<bigint> {
  const stmt = await conn.prepare(
    `SELECT MAX(source_seq) AS max_seq FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ?`,
  )
  const row = (await stmt.get(sourceNodeId)) as { max_seq?: SeqValue } | undefined
  return toSeq(row?.max_seq)
}

export async function maxAppliedSourceSeqByNode(conn: SQLiteConnection): Promise<Map<string, bigint>> {
  const stmt = await conn.prepare(
    `SELECT source_node_id, MAX(source_seq) AS max_seq FROM ${APPLIED_CHANGES_TABLE} GROUP BY source_node_id`,
  )
  const rows = (await stmt.all()) as Array<{ source_node_id: string; max_seq: SeqValue }>
  const byNode = new Map<string, bigint>()
  for (const row of rows) {
    if (row.max_seq === null || row.max_seq === undefined) continue
    byNode.set(row.source_node_id, toSeq(row.max_seq))
  }
  return byNode
}

export async function appliedSourceSeqsInRange(
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
