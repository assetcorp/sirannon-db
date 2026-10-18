import type { SQLiteConnection } from '../driver/types.js'
import { PEER_STATE_TABLE } from '../internal-tables.js'
import { seqFromColumn } from './changes-table.js'

/**
 * Creates the table that holds the highest change-log sequence that each replication peer has acknowledged, where
 * the table is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensurePeerStateTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${PEER_STATE_TABLE}" (
  peer_node_id TEXT PRIMARY KEY,
  last_acked_seq INTEGER NOT NULL DEFAULT 0,
  last_received_hlc TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
)`)
}

/**
 * Records a peer's acknowledgement and keeps the higher of the stored and the acknowledged sequence. The stored time
 * changes only when the acknowledged sequence is at least the stored one.
 *
 * @param conn - The writer connection.
 * @param peerNodeId - The peer.
 * @param seq - The sequence that the peer acknowledged.
 * @param updatedAt - The time of the acknowledgement, in seconds since the Unix epoch.
 * @returns A promise that settles once SQLite has written the row.
 * @internal
 */
export async function upsertPeerAckedSeq(
  conn: SQLiteConnection,
  peerNodeId: string,
  seq: bigint,
  updatedAt: number,
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO ${PEER_STATE_TABLE} (peer_node_id, last_acked_seq, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(peer_node_id)
     DO UPDATE SET
       last_acked_seq = max(${PEER_STATE_TABLE}.last_acked_seq, excluded.last_acked_seq),
       updated_at = CASE
         WHEN excluded.last_acked_seq >= ${PEER_STATE_TABLE}.last_acked_seq THEN excluded.updated_at
         ELSE ${PEER_STATE_TABLE}.updated_at
       END`,
  )
  await stmt.run(peerNodeId, seq.toString(), updatedAt)
}

/**
 * Deletes the recorded progress of every peer.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once SQLite has deleted the rows.
 * @internal
 */
export async function deletePeerStates(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM ${PEER_STATE_TABLE}`)
}

/**
 * Deletes the recorded progress of one peer.
 *
 * @param conn - The writer connection.
 * @param peerNodeId - The peer.
 * @returns A promise that settles once SQLite has deleted the row.
 * @internal
 */
export async function deletePeerState(conn: SQLiteConnection, peerNodeId: string): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM ${PEER_STATE_TABLE} WHERE peer_node_id = ?`)
  await stmt.run(peerNodeId)
}

/**
 * Returns the highest sequence that one peer has acknowledged.
 *
 * @param conn - The connection to read the table through.
 * @param peerNodeId - The peer.
 * @returns That sequence, or zero when the table holds no row for the peer.
 * @internal
 */
export async function selectPeerAckedSeq(conn: SQLiteConnection, peerNodeId: string): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT last_acked_seq FROM ${PEER_STATE_TABLE} WHERE peer_node_id = ?`)
  const row = (await stmt.get(peerNodeId)) as { last_acked_seq?: unknown } | undefined
  return seqFromColumn(row?.last_acked_seq)
}

/**
 * Returns the lowest of the sequences that the peers have acknowledged, which is the point up to which every peer
 * holds the change log.
 *
 * @param conn - The connection to read the table through.
 * @returns That sequence, or `null` when the table holds no peer.
 * @internal
 */
export async function selectMinPeerAckedSeq(conn: SQLiteConnection): Promise<bigint | null> {
  const stmt = await conn.prepare(`SELECT MIN(last_acked_seq) AS min_seq, COUNT(*) AS cnt FROM ${PEER_STATE_TABLE}`)
  const row = (await stmt.get()) as { min_seq?: unknown; cnt?: number | bigint } | undefined
  if (row === undefined || Number(row.cnt ?? 0) === 0) return null
  return seqFromColumn(row.min_seq)
}
