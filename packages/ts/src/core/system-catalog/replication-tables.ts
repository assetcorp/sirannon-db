import type { SQLiteConnection } from '../driver/types.js'
import { APPLIED_CHANGES_TABLE, COLUMN_VERSIONS_TABLE, PEER_STATE_TABLE, SYNC_STATE_TABLE } from '../internal-tables.js'

export async function ensureBatchApplyTables(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${APPLIED_CHANGES_TABLE}" (
  source_node_id TEXT NOT NULL,
  source_seq INTEGER NOT NULL,
  applied_at REAL NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
)`)

  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${COLUMN_VERSIONS_TABLE}" (
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  column_name TEXT NOT NULL,
  hlc TEXT NOT NULL,
  node_id TEXT NOT NULL,
  PRIMARY KEY (table_name, row_id, column_name)
)`)
}

export async function ensureReplicationStateTables(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${PEER_STATE_TABLE}" (
  peer_node_id TEXT PRIMARY KEY,
  last_acked_seq INTEGER NOT NULL DEFAULT 0,
  last_received_hlc TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
)`)

  await ensureBatchApplyTables(conn)

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
