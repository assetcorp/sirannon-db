import type { SQLiteConnection } from '../driver/types.js'
import { PEER_STATE_TABLE } from '../internal-tables.js'

type SeqValue = number | bigint | string | null | undefined

export async function ensurePeerStateTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${PEER_STATE_TABLE}" (
  peer_node_id TEXT PRIMARY KEY,
  last_acked_seq INTEGER NOT NULL DEFAULT 0,
  last_received_hlc TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
)`)
}

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

export async function selectPeerAckedSeq(conn: SQLiteConnection, peerNodeId: string): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT last_acked_seq FROM ${PEER_STATE_TABLE} WHERE peer_node_id = ?`)
  const row = (await stmt.get(peerNodeId)) as { last_acked_seq?: SeqValue } | undefined
  const seq = row?.last_acked_seq
  if (seq === null || seq === undefined) return 0n
  return typeof seq === 'bigint' ? seq : BigInt(String(seq))
}

export async function selectMinPeerAckedSeq(conn: SQLiteConnection): Promise<bigint | null> {
  const stmt = await conn.prepare(`SELECT MIN(last_acked_seq) AS min_seq, COUNT(*) AS cnt FROM ${PEER_STATE_TABLE}`)
  const row = (await stmt.get()) as { min_seq?: SeqValue; cnt?: number | bigint } | undefined
  if (row === undefined || Number(row.cnt ?? 0) === 0) return null

  const min = row.min_seq
  if (min === null || min === undefined) return 0n
  return typeof min === 'bigint' ? min : BigInt(String(min))
}
