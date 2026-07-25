import type { SQLiteConnection } from '../driver/types.js'
import { SYNC_STATE_TABLE } from '../internal-tables.js'

const SYNC_META_ROW = '__sync_meta__'

type SeqValue = number | bigint | string | null | undefined

export interface SyncMetaRow {
  status: string
  snapshotSeq: bigint | null
  sourcePeerId: string | null
}

export async function ensureSyncStateTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${SYNC_STATE_TABLE}" (
  table_name TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending',
  row_count INTEGER NOT NULL DEFAULT 0,
  pk_hash TEXT NOT NULL DEFAULT '',
  snapshot_seq INTEGER,
  source_peer_id TEXT,
  started_at REAL,
  completed_at REAL,
  request_id TEXT
)`)
}

export async function upsertSyncTableStatus(
  conn: SQLiteConnection,
  entry: { tableName: string; status: string; rowCount: number; pkHash: string; completedAt: number | null },
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO ${SYNC_STATE_TABLE} (table_name, status, row_count, pk_hash, completed_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(table_name) DO UPDATE SET
       status = excluded.status,
       row_count = COALESCE(excluded.row_count, row_count),
       pk_hash = COALESCE(excluded.pk_hash, pk_hash),
       completed_at = excluded.completed_at`,
  )
  await stmt.run(entry.tableName, entry.status, entry.rowCount, entry.pkHash, entry.completedAt)
}

export async function upsertSyncMeta(
  conn: SQLiteConnection,
  entry: {
    status: string
    snapshotSeq: bigint | undefined
    sourcePeerId: string | undefined
    startedAt: number | null
    requestId: string | undefined
  },
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO ${SYNC_STATE_TABLE} (table_name, status, snapshot_seq, source_peer_id, started_at, request_id)
     VALUES ('${SYNC_META_ROW}', ?, ?, ?, ?, ?)
     ON CONFLICT(table_name) DO UPDATE SET
       status = excluded.status,
       snapshot_seq = COALESCE(excluded.snapshot_seq, snapshot_seq),
       source_peer_id = COALESCE(excluded.source_peer_id, source_peer_id),
       started_at = COALESCE(excluded.started_at, started_at),
       request_id = COALESCE(excluded.request_id, request_id)`,
  )
  await stmt.run(
    entry.status,
    entry.snapshotSeq !== undefined ? entry.snapshotSeq.toString() : null,
    entry.sourcePeerId ?? null,
    entry.startedAt,
    entry.requestId ?? null,
  )
}

export async function syncMetaRow(conn: SQLiteConnection): Promise<SyncMetaRow | null> {
  const stmt = await conn.prepare(
    `SELECT status, snapshot_seq, source_peer_id FROM ${SYNC_STATE_TABLE} WHERE table_name = '${SYNC_META_ROW}'`,
  )
  const row = (await stmt.get()) as
    | { status: string; snapshot_seq: SeqValue; source_peer_id: string | null }
    | undefined
  if (!row) return null

  const seq = row.snapshot_seq
  return {
    status: row.status,
    snapshotSeq: seq === null || seq === undefined ? null : typeof seq === 'bigint' ? seq : BigInt(String(seq)),
    sourcePeerId: row.source_peer_id,
  }
}

export async function completedSyncTableNames(conn: SQLiteConnection): Promise<string[]> {
  const stmt = await conn.prepare(
    `SELECT table_name FROM ${SYNC_STATE_TABLE} WHERE table_name != '${SYNC_META_ROW}' AND status = 'completed'`,
  )
  const rows = (await stmt.all()) as Array<{ table_name: string }>
  return rows.map(row => row.table_name)
}

export async function deleteSyncTableStates(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM ${SYNC_STATE_TABLE} WHERE table_name != '${SYNC_META_ROW}'`)
}
