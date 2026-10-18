import type { SQLiteConnection } from '../driver/types.js'
import { ensureMetaTable, selectMetaValue, upsertMetaValue } from './meta-table.js'

const PRIMARY_TERM_START_META_KEY = 'replication_primary_term_start'
const ANCHORED_TERM_META_KEY = 'replication_anchored_term'
const HISTORY_BASE_META_KEY = 'replication_history_base'
const QUARANTINED_META_KEY = 'replication_quarantined'
const DECIMAL_DIGITS = /^\d+$/

/**
 * Identifies one transaction of a replication group's history by its `txId` and its highest HLC.
 *
 * @internal
 */
export interface StoredHistoryPoint {
  /** Holds the highest HLC among the transaction's changes. */
  hlc: string
  /** Identifies the transaction. */
  txId: string
}

/**
 * Locates the start of a node's term as primary in that node's change log.
 *
 * @internal
 */
export interface StoredTermStart {
  /** Holds the primary term. */
  term: bigint
  /** Holds the newest history position in the change log when the term began. */
  seq: bigint
}

function parseTermStart(raw: string | null): StoredTermStart | null {
  if (raw === null) return null
  try {
    const parsed: unknown = JSON.parse(raw)
    if (typeof parsed !== 'object' || parsed === null) return null
    const term: unknown = Reflect.get(parsed, 'term')
    const seq: unknown = Reflect.get(parsed, 'seq')
    if (typeof term !== 'string' || typeof seq !== 'string') return null
    if (!DECIMAL_DIGITS.test(term) || !DECIMAL_DIGITS.test(seq)) return null
    return { term: BigInt(term), seq: BigInt(seq) }
  } catch {
    return null
  }
}

function parseHistoryPoint(raw: string | null): StoredHistoryPoint | null {
  if (raw === null) return null
  try {
    const parsed: unknown = JSON.parse(raw)
    if (typeof parsed !== 'object' || parsed === null) return null
    const hlc: unknown = Reflect.get(parsed, 'hlc')
    const txId: unknown = Reflect.get(parsed, 'txId')
    if (typeof hlc !== 'string' || typeof txId !== 'string' || hlc === '') return null
    return { hlc, txId }
  } catch {
    return null
  }
}

/**
 * Returns where this node's term as primary began in its change log, or null before the node records a term start.
 *
 * @param conn - The connection to read through.
 * @returns The recorded term and position, or null when none is recorded or the stored value is malformed.
 *
 * @internal
 */
export async function selectPrimaryTermStart(conn: SQLiteConnection): Promise<StoredTermStart | null> {
  await ensureMetaTable(conn)
  return parseTermStart(await selectMetaValue(conn, PRIMARY_TERM_START_META_KEY))
}

/**
 * Records where this node's term as primary begins in its change log.
 *
 * @param conn - The connection to write through.
 * @param termStart - The term and the newest history position in the change log when the term began.
 *
 * @internal
 */
export async function upsertPrimaryTermStart(conn: SQLiteConnection, termStart: StoredTermStart): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(
    conn,
    PRIMARY_TERM_START_META_KEY,
    JSON.stringify({ term: termStart.term.toString(), seq: termStart.seq.toString() }),
  )
}

/**
 * Returns the primary term whose history this node last reconciled its own with, or null before the first time.
 *
 * @param conn - The connection to read through.
 * @returns The recorded term, or null when none is recorded or the stored value is malformed.
 *
 * @internal
 */
export async function selectAnchoredTerm(conn: SQLiteConnection): Promise<bigint | null> {
  await ensureMetaTable(conn)
  const raw = await selectMetaValue(conn, ANCHORED_TERM_META_KEY)
  return raw !== null && DECIMAL_DIGITS.test(raw) ? BigInt(raw) : null
}

/**
 * Records the primary term whose history this node has reconciled its own with.
 *
 * @param conn - The connection to write through.
 * @param term - The reconciled term.
 *
 * @internal
 */
export async function upsertAnchoredTerm(conn: SQLiteConnection, term: bigint): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, ANCHORED_TERM_META_KEY, term.toString())
}

/**
 * Returns the history transaction that a full copy left this node holding without a change-log entry, or null when no
 * copy recorded one.
 *
 * @param conn - The connection to read through.
 * @returns That transaction, or null.
 *
 * @internal
 */
export async function selectHistoryBase(conn: SQLiteConnection): Promise<StoredHistoryPoint | null> {
  await ensureMetaTable(conn)
  return parseHistoryPoint(await selectMetaValue(conn, HISTORY_BASE_META_KEY))
}

/**
 * Records the history transaction that a full copy left this node holding, or forgets the recorded one when the copy
 * named none.
 *
 * @param conn - The connection to write through.
 * @param point - The newest history transaction in the copy, or null.
 *
 * @internal
 */
export async function upsertHistoryBase(conn: SQLiteConnection, point: StoredHistoryPoint | null): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, HISTORY_BASE_META_KEY, point ? JSON.stringify({ hlc: point.hlc, txId: point.txId }) : '')
}

/**
 * Returns whether this node's database holds writes for which the group quarantined it. The record stays true from
 * the moment that the node finds itself faulted until a full copy replaces its rows.
 *
 * @param conn - The connection to read through.
 * @returns True while the quarantined writes remain.
 *
 * @internal
 */
export async function selectQuarantined(conn: SQLiteConnection): Promise<boolean> {
  await ensureMetaTable(conn)
  return (await selectMetaValue(conn, QUARANTINED_META_KEY)) === 'true'
}

/**
 * Records whether this node's database holds writes for which the group quarantined it.
 *
 * @param conn - The connection to write through.
 * @param quarantined - True once the node finds itself faulted, and false once a full copy replaces its rows.
 *
 * @internal
 */
export async function upsertQuarantined(conn: SQLiteConnection, quarantined: boolean): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, QUARANTINED_META_KEY, quarantined ? 'true' : 'false')
}
