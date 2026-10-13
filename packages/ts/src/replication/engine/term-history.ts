import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import {
  deletePeerStates,
  ensureMetaTable,
  selectHistoryTransaction,
  selectMaxHistoryChangeSeq,
  selectMetaValue,
  selectNewestHistoryChange,
  upsertMetaValue,
} from '../../core/system-catalog/index.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError } from '../errors.js'
import type { SyncHistoryPoint, SyncRequest } from '../types.js'
import {
  applyCoordinatorState,
  errorDetails,
  hasCurrentPrimaryAuthorityFor,
  refreshCoordinatorState,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { unrefTimer } from './timers.js'

const PRIMARY_TERM_START_META_KEY = 'replication_primary_term_start'
const ANCHORED_TERM_META_KEY = 'replication_anchored_term'
const HISTORY_BASE_META_KEY = 'replication_history_base'
const QUARANTINED_META_KEY = 'replication_quarantined'
const IN_SYNC_UPDATE_ATTEMPTS = 3
const DECIMAL_DIGITS = /^\d+$/

/**
 * Holds where this node's change log meets the replication group's history: the term whose primary this node last
 * reconciled its history with, and, while this node is primary, the log position at which its term began.
 *
 * @internal
 */
export class TermHistory {
  /** Is true once the engine has finished starting, so that no reconciliation begins while startup picks the sync phase. */
  active = false
  /** Holds the primary term whose history this node last reconciled its own with, or null before the first time. */
  anchoredTerm: bigint | null = null
  /** Is true when this node held primary authority in its reconciled term, so that only the former-primary path rejoins it. */
  heldPrimaryInAnchoredTerm = false
  /** Holds the term whose start this node recorded as primary, or null before it records one. */
  termStartTerm: bigint | null = null
  /** Holds the newest history position in this node's change log when its term as primary began. */
  termStartSeq = 0n
  /** Is true from the moment this replica asks a new primary where their histories meet until it holds that history. */
  reconciling = false
  /** Identifies the sync request that named this replica's newest history transaction, or is null when none is pending. */
  resumeRequestId: string | null = null
  /** Holds the primary term of the pending request that names this replica's newest history transaction. */
  resumeRequestTerm: bigint | null = null
  /** Holds the time in milliseconds at which this replica sent the pending request, so that it resends a lost one. */
  resumeRequestedAt = 0
  /** Holds the term-start recording in progress, so that concurrent callers wait for one recording. */
  pendingTermStart: Promise<void> | null = null
  private exclusiveQueue: Promise<unknown> = Promise.resolve()

  /**
   * Runs `fn` after every function that an earlier call queued has settled, so that an inbound batch and a history
   * rollback never write at the same time.
   *
   * @param fn - The function to run.
   * @returns The value that `fn` resolves to.
   */
  exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const ticket = this.exclusiveQueue.then(fn)
    this.exclusiveQueue = ticket.catch(() => undefined)
    return ticket
  }
}

/**
 * Returns the node IDs whose stamped changes form the replication group's history in this node's change log.
 *
 * @param engine - The engine whose group membership names the nodes.
 * @returns This node's ID and the group's voting data-bearing node IDs.
 */
export function historyOrigins(engine: ReplicationEngine): string[] {
  const voters = engine.coordinatorState?.votingDataBearingNodeIds ?? []
  return voters.includes(engine.nodeId) ? [...voters] : [engine.nodeId, ...voters]
}

function parseTermStart(raw: string | null): { term: bigint; seq: bigint } | null {
  if (raw === null) return null
  try {
    const parsed = JSON.parse(raw) as { term?: unknown; seq?: unknown }
    if (typeof parsed.term !== 'string' || typeof parsed.seq !== 'string') return null
    if (!DECIMAL_DIGITS.test(parsed.term) || !DECIMAL_DIGITS.test(parsed.seq)) return null
    return { term: BigInt(parsed.term), seq: BigInt(parsed.seq) }
  } catch {
    return null
  }
}

function parseHistoryPoint(raw: string | null): SyncHistoryPoint | null {
  if (raw === null) return null
  try {
    const parsed = JSON.parse(raw) as { hlc?: unknown; txId?: unknown }
    if (typeof parsed.hlc !== 'string' || typeof parsed.txId !== 'string' || parsed.hlc === '') return null
    return { hlc: parsed.hlc, txId: parsed.txId }
  } catch {
    return null
  }
}

/**
 * Returns the history transaction that a full copy left this node holding without a change-log entry, or null when no
 * copy recorded one.
 *
 * @param conn - The connection to read through.
 * @returns That transaction, or null.
 */
export async function selectHistoryBase(conn: SQLiteConnection): Promise<SyncHistoryPoint | null> {
  await ensureMetaTable(conn)
  return parseHistoryPoint(await selectMetaValue(conn, HISTORY_BASE_META_KEY))
}

/**
 * Records the history transaction that a full copy left this node holding, or forgets the recorded one when the copy
 * named none.
 *
 * @param conn - The connection to write through.
 * @param point - The newest history transaction in the copy, or null.
 */
export async function upsertHistoryBase(conn: SQLiteConnection, point: SyncHistoryPoint | null): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, HISTORY_BASE_META_KEY, point ? JSON.stringify(point) : '')
}

/**
 * Returns whether this node's database still holds the writes for which the group quarantined it, which stays true
 * from the moment the node marks itself faulted until a full copy replaces its rows.
 *
 * @param conn - The connection to read through.
 * @returns True while the quarantined writes remain.
 */
export async function selectQuarantined(conn: SQLiteConnection): Promise<boolean> {
  await ensureMetaTable(conn)
  return (await selectMetaValue(conn, QUARANTINED_META_KEY)) === 'true'
}

/**
 * Records whether this node's database holds writes for which the group quarantined it.
 *
 * @param conn - The connection to write through.
 * @param quarantined - True when the node marks itself faulted, and false once a full copy replaces its rows.
 */
export async function upsertQuarantined(conn: SQLiteConnection, quarantined: boolean): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, QUARANTINED_META_KEY, quarantined ? 'true' : 'false')
}

/**
 * Records the primary term whose history this node has just reconciled its own with.
 *
 * @param conn - The connection to write through.
 * @param term - The reconciled term.
 */
export async function upsertAnchoredTerm(conn: SQLiteConnection, term: bigint): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, ANCHORED_TERM_META_KEY, term.toString())
}

/**
 * Loads the reconciled primary term from the database into the engine. A node whose copy is complete and that holds
 * no recorded term, which a node written by an earlier release does, takes the current term, so that an upgrade
 * reconciles nothing.
 *
 * @param engine - The engine to load into.
 */
export async function loadTermHistory(engine: ReplicationEngine): Promise<void> {
  if (!engine.isCoordinatorMode()) return
  const conn = engine.writerConn
  await ensureMetaTable(conn)
  const raw = await selectMetaValue(conn, ANCHORED_TERM_META_KEY)
  if (raw !== null && DECIMAL_DIGITS.test(raw)) {
    const anchoredTerm = BigInt(raw)
    const termStart = parseTermStart(await selectMetaValue(conn, PRIMARY_TERM_START_META_KEY))
    engine.history.anchoredTerm = anchoredTerm
    engine.history.heldPrimaryInAnchoredTerm = termStart !== null && termStart.term === anchoredTerm
    return
  }
  const state = engine.coordinatorState
  if (!state || !(await engine.log.isSyncCompleted())) return
  engine.history.anchoredTerm = state.primaryTerm
  await upsertAnchoredTerm(conn, state.primaryTerm)
}

/**
 * Records, once per term, where this node's term as primary begins: it resets every peer's progress, because a
 * replica's acknowledgements from an earlier term count positions in another node's log, and it raises the group's
 * durability point to the newest history position in this node's log, so that the node admits no replica to the
 * in-sync set before the replica holds that history. The node also records the term as the one whose history it holds,
 * so that it reconciles its history with the next primary's once it loses authority.
 *
 * @param engine - The engine of the node that holds primary authority.
 * @returns A promise that settles once the term start is recorded, or at once when this node is not primary.
 */
export function ensurePrimaryTermStart(engine: ReplicationEngine): Promise<void> {
  const state = engine.coordinatorState
  const history = engine.history
  if (!state || !hasCurrentPrimaryAuthorityFor(engine, state)) return Promise.resolve()
  if (history.termStartTerm === state.primaryTerm) return Promise.resolve()
  if (history.pendingTermStart) return history.pendingTermStart
  const pending = recordPrimaryTermStart(engine, state).finally(() => {
    history.pendingTermStart = null
  })
  history.pendingTermStart = pending
  return pending
}

async function recordPrimaryTermStart(engine: ReplicationEngine, state: ReplicationGroupState): Promise<void> {
  const conn = engine.writerConn
  const term = state.primaryTerm
  await ensureMetaTable(conn)
  const recorded = parseTermStart(await selectMetaValue(conn, PRIMARY_TERM_START_META_KEY))
  let seq: bigint
  if (recorded !== null && recorded.term === term) {
    seq = recorded.seq
  } else {
    seq = await selectMaxHistoryChangeSeq(conn, CHANGES_TABLE, historyOrigins(engine))
    await deletePeerStates(conn)
    engine.peerTracker.restartProgress(seq)
    await upsertMetaValue(
      conn,
      PRIMARY_TERM_START_META_KEY,
      JSON.stringify({ term: term.toString(), seq: seq.toString() }),
    )
  }
  if (!(await raiseDurabilityPoint(engine, term, seq))) return
  await upsertAnchoredTerm(conn, term)
  engine.history.anchoredTerm = term
  engine.history.heldPrimaryInAnchoredTerm = true
  engine.history.termStartSeq = seq
  engine.history.termStartTerm = term
}

async function raiseDurabilityPoint(engine: ReplicationEngine, term: bigint, seq: bigint): Promise<boolean> {
  const config = engine.config.coordinator
  if (!config) return false
  for (let attempt = 1; ; attempt++) {
    const fresh = await refreshCoordinatorState(engine)
    if (!fresh || fresh.primaryTerm !== term || !hasCurrentPrimaryAuthorityFor(engine, fresh)) return false
    if (fresh.durabilityPointSeq >= seq) return true
    try {
      const updated = await config.coordinator.updateInSyncSet({
        clusterId: config.clusterId,
        groupId: config.groupId,
        inSyncNodeIds: fresh.inSyncNodeIds,
        durabilityPointSeq: seq,
      })
      if (!updated) return false
      applyCoordinatorState(engine, updated)
      return updated.primaryTerm === term
    } catch (err: unknown) {
      if (!(err instanceof RangeError)) throw err
      if (attempt >= IN_SYNC_UPDATE_ATTEMPTS) {
        throw new CoordinatorError('The in-sync set kept changing while this node recorded the start of its term', {
          ...errorDetails(engine, fresh),
          cause: err.message,
        })
      }
    }
  }
}

/**
 * Returns the newest history transaction that a connection sees, which a full copy names so that the joiner can later
 * resume its history from the copy.
 *
 * @param engine - The engine whose group membership names the history's nodes.
 * @param conn - The connection that holds the copy's read snapshot.
 * @returns That transaction, or undefined outside coordinator mode or when the log holds no history.
 */
export async function selectCopyHistoryPoint(
  engine: ReplicationEngine,
  conn: SQLiteConnection,
): Promise<SyncHistoryPoint | undefined> {
  if (!engine.isCoordinatorMode()) return undefined
  const newest = await selectNewestHistoryChange(conn, CHANGES_TABLE, historyOrigins(engine))
  return newest ? { hlc: newest.hlc, txId: newest.txId } : undefined
}

async function selectSharedHistoryPoint(
  engine: ReplicationEngine,
  tail: SyncHistoryPoint | undefined,
): Promise<{ point: SyncHistoryPoint; seq: bigint } | null> {
  const state = engine.coordinatorState
  if (!tail || !state || engine.history.termStartTerm !== state.primaryTerm) return null
  const conn = engine.writerConn
  const held = await selectHistoryTransaction(conn, CHANGES_TABLE, tail.txId)
  if (held !== null) return { point: { hlc: held.hlc, txId: held.txId }, seq: held.seq }
  const shared = await selectNewestHistoryChange(conn, CHANGES_TABLE, historyOrigins(engine), {
    maxSeq: engine.history.termStartSeq,
    beforeHlc: tail.hlc,
  })
  return shared ? { point: { hlc: shared.hlc, txId: shared.txId }, seq: shared.seq } : null
}

async function removeFromInSyncSet(engine: ReplicationEngine, nodeId: string): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  for (let attempt = 1; ; attempt++) {
    const fresh = await refreshCoordinatorState(engine)
    if (!fresh || !fresh.inSyncNodeIds.includes(nodeId)) return
    try {
      const updated = await config.coordinator.updateInSyncSet({
        clusterId: config.clusterId,
        groupId: config.groupId,
        inSyncNodeIds: fresh.inSyncNodeIds.filter(id => id !== nodeId),
      })
      if (updated) applyCoordinatorState(engine, updated)
      return
    } catch (err: unknown) {
      if (!(err instanceof RangeError)) throw err
      if (attempt >= IN_SYNC_UPDATE_ATTEMPTS) {
        throw new CoordinatorError('The in-sync set kept changing while this node removed a node that needs a copy', {
          ...errorDetails(engine, fresh),
          cause: err.message,
        })
      }
    }
  }
}

/**
 * Answers a sync request that names the joiner's newest history transaction without streaming a copy.
 *
 * When this primary's log holds that transaction or an earlier transaction from before this node's term, the answer
 * is resumed: it names the last transaction that both nodes hold and the position after which this node resends its
 * history, and this node keeps that history from pruning until the joiner reports its position or the sync deadline
 * passes. Otherwise the answer tells the joiner to ask for a full copy. Before it tells a joiner to copy, and before it
 * streams a copy for a request that names no transaction, this node removes the joiner from the in-sync set, because
 * the copy replaces the joiner's rows and the joiner must not be promoted while it holds part of a copy. This node
 * sends no answer to a request that names a transaction before it has recorded the start of its term, and the joiner
 * then asks again.
 *
 * @param engine - The engine of the primary that received the request.
 * @param request - The joiner's request.
 * @param fromPeerId - The joiner's node ID.
 * @returns True when this node sent the answer, and false when the request asks for a full copy.
 */
export async function answerSyncFromHistory(
  engine: ReplicationEngine,
  request: SyncRequest,
  fromPeerId: string,
): Promise<boolean> {
  if (request.historyTail && engine.history.termStartTerm !== engine.coordinatorState?.primaryTerm) return true
  const shared = await selectSharedHistoryPoint(engine, request.historyTail)
  if (shared === null) {
    await removeFromInSyncSet(engine, fromPeerId)
    if (!request.historyTail) return false
    await engine.config.transport.sendSyncComplete(
      fromPeerId,
      engine.decorate({ requestId: request.requestId, snapshotSeq: 0n, manifests: [], resumed: false }),
    )
    return true
  }
  const heldSeq = shared.seq
  const point = shared.point
  engine.log.registerActiveSyncSeq(heldSeq)
  unrefTimer(setTimeout(() => engine.log.unregisterActiveSyncSeq(heldSeq), engine.maxSyncDurationMs))
  await engine.config.transport.sendSyncComplete(
    fromPeerId,
    engine.decorate({
      requestId: request.requestId,
      snapshotSeq: heldSeq,
      manifests: [],
      historyPoint: point,
      resumed: true,
    }),
  )
  return true
}
