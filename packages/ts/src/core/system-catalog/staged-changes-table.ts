import type { SQLiteConnection } from '../driver/types.js'
import { STAGED_CHANGES_TABLE } from '../internal-tables.js'

type SeqValue = number | bigint | string | null | undefined

function toSeq(value: SeqValue): bigint | null {
  if (value === null || value === undefined) return null
  return typeof value === 'bigint' ? value : BigInt(String(value))
}

export interface StagedChangeRow {
  seq: string
  table_name: string
  operation: string
  row_id: string
  changed_at: number
  old_data: string | null
  new_data: string | null
  node_id: string
  tx_id: string
  hlc: string
  tx_end: number
}

const STAGED_COLUMNS = 'seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc, tx_end'

export async function ensureStagedChangesTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${STAGED_CHANGES_TABLE}" (
  seq INTEGER PRIMARY KEY,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL,
  old_data TEXT,
  new_data TEXT,
  node_id TEXT NOT NULL DEFAULT '',
  tx_id TEXT NOT NULL DEFAULT '',
  hlc TEXT NOT NULL DEFAULT '',
  tx_end INTEGER NOT NULL DEFAULT 0
)`)
  await conn.exec(
    `CREATE INDEX IF NOT EXISTS "idx_${STAGED_CHANGES_TABLE}_tx_end" ON "${STAGED_CHANGES_TABLE}" (tx_end, seq)`,
  )
}

export function upsertStagedChangeSql(): string {
  return `INSERT INTO "${STAGED_CHANGES_TABLE}" (${STAGED_COLUMNS})
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(seq) DO UPDATE SET
            table_name = excluded.table_name,
            operation = excluded.operation,
            row_id = excluded.row_id,
            changed_at = excluded.changed_at,
            old_data = excluded.old_data,
            new_data = excluded.new_data,
            node_id = excluded.node_id,
            tx_id = excluded.tx_id,
            hlc = excluded.hlc,
            tx_end = excluded.tx_end`
}

export async function selectMaxStagedSeq(conn: SQLiteConnection): Promise<bigint | null> {
  const stmt = await conn.prepare(`SELECT MAX(seq) AS seq FROM "${STAGED_CHANGES_TABLE}"`)
  const row = (await stmt.get()) as { seq?: SeqValue } | undefined
  return toSeq(row?.seq)
}

export async function selectFirstStagedTransactionEnd(
  conn: SQLiteConnection,
): Promise<{ seq: bigint; txId: string } | null> {
  const stmt = await conn.prepare(
    `SELECT seq, tx_id FROM "${STAGED_CHANGES_TABLE}" WHERE tx_end = 1 ORDER BY seq ASC LIMIT 1`,
  )
  const row = (await stmt.get()) as { seq?: SeqValue; tx_id?: string } | undefined
  const seq = toSeq(row?.seq)
  if (seq === null) return null
  return { seq, txId: row?.tx_id ?? '' }
}

export async function selectStagedChangesInRange(
  conn: SQLiteConnection,
  afterSeq: bigint,
  upToSeq: bigint,
  limit: number,
): Promise<StagedChangeRow[]> {
  const stmt = await conn.prepare(
    `SELECT ${STAGED_COLUMNS} FROM "${STAGED_CHANGES_TABLE}"
     WHERE seq > ? AND seq <= ? ORDER BY seq ASC LIMIT ?`,
  )
  return (await stmt.all(afterSeq.toString(), upToSeq.toString(), limit)) as StagedChangeRow[]
}

export async function deleteStagedChangesUpToSeq(conn: SQLiteConnection, upToSeq: bigint): Promise<void> {
  const stmt = await conn.prepare(`DELETE FROM "${STAGED_CHANGES_TABLE}" WHERE seq <= ?`)
  await stmt.run(upToSeq.toString())
}

export async function deleteAllStagedChanges(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`DELETE FROM "${STAGED_CHANGES_TABLE}"`)
}
