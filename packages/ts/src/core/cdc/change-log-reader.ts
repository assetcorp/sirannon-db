import type { SQLiteConnection } from '../driver/types.js'
import {
  selectChangesAfterSeqSql,
  selectTableChangesInRangeSql,
  selectTablesChangesInRangeSql,
} from '../system-catalog/index.js'
import type { ChangeEvent } from '../types.js'
import { decodeTaggedValues } from './encoding.js'
import type { StatementCache } from './statement-cache.js'
import type { ChangeRow } from './types.js'

export interface PollResult {
  events: ChangeEvent[]
  lastSeq: bigint
  atTxBoundary: boolean
}

export function rowToEvent(row: ChangeRow): ChangeEvent {
  return {
    type: row.operation.toLowerCase() as 'insert' | 'update' | 'delete',
    table: row.table_name,
    row: row.new_data ? (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>) : {},
    oldRow: row.old_data ? (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>) : undefined,
    seq: BigInt(row.seq),
    timestamp: row.changed_at,
    rowId: String(row.row_id),
    ...(row.node_id ? { origin: row.node_id } : {}),
    ...(row.hlc ? { hlc: row.hlc } : {}),
    ...(row.tx_id ? { txId: row.tx_id } : {}),
  }
}

export async function pollChanges(
  conn: SQLiteConnection,
  cache: StatementCache,
  changesTable: string,
  afterSeq: bigint,
  batchSize: number,
): Promise<PollResult | null> {
  const stmt = await cache.get(conn, 'poll', selectChangesAfterSeqSql(changesTable))

  const rows = (await stmt.all(afterSeq.toString(), batchSize + 1)) as ChangeRow[]
  if (rows.length === 0) return null

  const emitted = rows.length > batchSize ? rows.slice(0, batchSize) : rows
  const lastEmitted = emitted[emitted.length - 1]
  const peekedBeyondBatch = rows[batchSize]

  return {
    events: emitted.map(rowToEvent),
    lastSeq: BigInt(lastEmitted.seq),
    atTxBoundary:
      peekedBeyondBatch === undefined || !lastEmitted.tx_id || (peekedBeyondBatch.tx_id ?? '') !== lastEmitted.tx_id,
  }
}

export async function readSinceOneTable(
  conn: SQLiteConnection,
  cache: StatementCache,
  changesTable: string,
  table: string,
  afterSeq: bigint,
  upToSeq: bigint,
  limit: number,
): Promise<ChangeEvent[]> {
  const stmt = await cache.get(conn, 'read_since', selectTableChangesInRangeSql(changesTable))
  const rows = (await stmt.all(table, afterSeq.toString(), upToSeq.toString(), limit)) as ChangeRow[]
  return rows.map(rowToEvent)
}

export async function readSinceTables(
  conn: SQLiteConnection,
  cache: StatementCache,
  changesTable: string,
  tables: readonly string[],
  afterSeq: bigint,
  upToSeq: bigint,
  limit: number,
): Promise<ChangeEvent[]> {
  if (tables.length === 0) return []

  const stmt = await cache.get(
    conn,
    `read_since_tables_${tables.length}`,
    selectTablesChangesInRangeSql(changesTable, tables.length),
  )
  const rows = (await stmt.all(...tables, afterSeq.toString(), upToSeq.toString(), limit)) as ChangeRow[]
  return rows.map(rowToEvent)
}
