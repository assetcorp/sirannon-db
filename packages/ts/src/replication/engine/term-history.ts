import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import {
  deletePeerStates,
  selectAnchoredTerm,
  selectHistoryTransaction,
  selectMaxHistoryChangeSeq,
  selectNewestHistoryChange,
  selectPrimaryTermStart,
  upsertAnchoredTerm,
  upsertPrimaryTermStart,
} from '../../core/system-catalog/index.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { StalePrimaryError } from '../errors.js'
import type { SyncHistoryPoint, SyncRequest } from '../types.js'
import { hasCurrentPrimaryAuthorityFor } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { updateInSyncSetFromCurrent } from './in-sync-updates.js'
import { unrefTimer } from './timers.js'

/**
 * Holds the sync request in which this replica names its newest history transaction to the primary, while the
 * replica waits for the answer.
 *
 * @internal
 */
export class ResumeRequest {
  /** Identifies the pending request, or is null when none is pending. */
  id: string | null = null
  /** Holds the primary term in which this replica sent the pending request. */
  term: bigint | null = null
  /** Holds the time in milliseconds at which this replica sent the pending request, so that it resends a lost one. */
  requestedAt = 0

  /**
   * Records a request that this replica sends now.
   *
   * @param id - The request ID.
   * @param term - The primary term in which the replica sends it.
   */
  start(id: string, term: bigint): void {
    this.id = id
    this.term = term
    this.requestedAt = Date.now()
  }

  /** Forgets the pending request, so that an answer to it no longer matches. */
  clear(): void {
    this.id = null
    this.term = null
    this.requestedAt = 0
  }

  /**
   * Returns true when a message carries the ID of the pending request.
   *
   * @param requestId - The request ID that the message carries.
   * @returns True while a request is pending and the IDs are equal.
   */
  matches(requestId: string): boolean {
    return this.id !== null && this.id === requestId
  }

  /**
   * Returns true when this replica sent a request in the given term less than `timeoutMs` ago, so that it waits for
   * that answer before it sends another request.
   *
   * @param term - The current primary term.
   * @param timeoutMs - How long, in milliseconds, the replica waits for an answer.
   * @returns True while that answer can still arrive.
   */
  awaitsAnswerIn(term: bigint, timeoutMs: number): boolean {
    return this.id !== null && this.term === term && Date.now() - this.requestedAt < timeoutMs
  }
}

/**
 * Holds where this node's change log meets the replication group's history. That covers the term whose primary this
 * node last reconciled its history with, and, while this node is primary, the log position at which its term began.
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
  /** Holds the pending request in which this replica names its newest history transaction. */
  readonly resumeRequest = new ResumeRequest()
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
 *
 * @internal
 */
export function historyOrigins(engine: ReplicationEngine): string[] {
  const voters = engine.coordinatorState?.votingDataBearingNodeIds ?? []
  return voters.includes(engine.nodeId) ? [...voters] : [engine.nodeId, ...voters]
}

/**
 * Loads the reconciled primary term from the database into the engine. When a node holds no recorded term, the engine
 * takes the current term if the node holds a complete copy, as a node that an earlier release wrote does, or if the
 * node starts with `initialSync` set to false, because the operator seeded its rows. An upgrade or a seeded start
 * therefore reconciles nothing, and the node reconciles its history at the next change of primary.
 *
 * @param engine - The engine to load into.
 *
 * @internal
 */
export async function loadTermHistory(engine: ReplicationEngine): Promise<void> {
  if (!engine.isCoordinatorMode()) return
  const conn = engine.writerConn
  const anchoredTerm = await selectAnchoredTerm(conn)
  if (anchoredTerm !== null) {
    const termStart = await selectPrimaryTermStart(conn)
    engine.history.anchoredTerm = anchoredTerm
    engine.history.heldPrimaryInAnchoredTerm = termStart !== null && termStart.term === anchoredTerm
    return
  }
  const state = engine.coordinatorState
  if (!state || (engine.initialSync && !(await engine.log.isSyncCompleted()))) return
  engine.history.anchoredTerm = state.primaryTerm
  await upsertAnchoredTerm(conn, state.primaryTerm)
}

/**
 * Returns a promise that settles once this node has recorded where its term as primary begins, which it does once per
 * term.
 *
 * The recording resets every peer's progress, because a replica's acknowledgements from an earlier term count
 * positions in another node's log. It also raises the group's durability point to the newest history position in this
 * node's log, so that the node admits no replica to the in-sync set before the replica holds that history. Last, the
 * node records the term as the one whose history it holds, so that it reconciles its history with the next primary's
 * once it loses authority.
 *
 * @param engine - The engine of the node that holds primary authority.
 * @returns A promise that settles once the term start is recorded, or at once when this node is not primary.
 *
 * @internal
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
  const recorded = await selectPrimaryTermStart(conn)
  let seq: bigint
  if (recorded !== null && recorded.term === term) {
    seq = recorded.seq
  } else {
    seq = await selectMaxHistoryChangeSeq(conn, CHANGES_TABLE, historyOrigins(engine))
    await deletePeerStates(conn)
    engine.peerTracker.restartProgress(seq)
    await upsertPrimaryTermStart(conn, { term, seq })
  }
  if (!(await raiseDurabilityPoint(engine, term, seq))) return
  await upsertAnchoredTerm(conn, term)
  engine.history.anchoredTerm = term
  engine.history.heldPrimaryInAnchoredTerm = true
  engine.history.termStartSeq = seq
  engine.history.termStartTerm = term
}

async function raiseDurabilityPoint(engine: ReplicationEngine, term: bigint, seq: bigint): Promise<boolean> {
  if (!engine.config.coordinator) return false
  try {
    await updateInSyncSetFromCurrent(engine, {
      purpose: 'the start of its term',
      primaryTerm: term,
      change: state =>
        state.durabilityPointSeq >= seq ? null : { inSyncNodeIds: state.inSyncNodeIds, durabilityPointSeq: seq },
    })
    return true
  } catch (err: unknown) {
    if (err instanceof StalePrimaryError) return false
    throw err
  }
}

/**
 * Returns the newest history transaction that a connection sees, which a full copy names so that the joiner can later
 * resume its history from the copy.
 *
 * @param engine - The engine whose group membership names the history's nodes.
 * @param conn - The connection that holds the copy's read snapshot.
 * @returns That transaction, or undefined outside coordinator mode or when the log holds no history.
 *
 * @internal
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

async function removeJoinerFromInSyncSet(engine: ReplicationEngine, joinerNodeId: string): Promise<void> {
  const primaryTerm = engine.coordinatorState?.primaryTerm
  if (primaryTerm === undefined) return
  await updateInSyncSetFromCurrent(engine, {
    purpose: 'the removal of a joiner that needs a full copy',
    primaryTerm,
    change: state =>
      state.inSyncNodeIds.includes(joinerNodeId)
        ? { inSyncNodeIds: state.inSyncNodeIds.filter(id => id !== joinerNodeId) }
        : null,
  })
}

/**
 * Answers a sync request that names the joiner's newest history transaction without streaming a copy.
 *
 * When this primary's log holds that transaction or an earlier transaction from before this node's term, the answer
 * is resumed. It names the last transaction that both nodes hold and the position after which this node resends its
 * history, and this node keeps that history from pruning until the joiner reports its position or the sync deadline
 * passes. Otherwise the answer tells the joiner to ask for a full copy.
 *
 * Before it tells a joiner to copy, and before it streams a copy for a request that names no transaction, this node
 * removes the joiner from the in-sync set, because the copy replaces the joiner's rows and the group must not promote
 * the joiner while it holds part of a copy. This node sends no answer to a request that names a transaction before it
 * has recorded the start of its term, and the joiner then asks again.
 *
 * @param engine - The engine of the primary that received the request.
 * @param request - The joiner's request.
 * @param fromPeerId - The joiner's node ID.
 * @returns True when this node sent the answer, and false when the request asks for a full copy.
 * @throws StalePrimaryError when this node loses primary duty before it removes the joiner from the in-sync set.
 *
 * @internal
 */
export async function answerSyncFromHistory(
  engine: ReplicationEngine,
  request: SyncRequest,
  fromPeerId: string,
): Promise<boolean> {
  if (request.historyTail && engine.history.termStartTerm !== engine.coordinatorState?.primaryTerm) return true
  const shared = await selectSharedHistoryPoint(engine, request.historyTail)
  if (shared === null) {
    await removeJoinerFromInSyncSet(engine, fromPeerId)
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
