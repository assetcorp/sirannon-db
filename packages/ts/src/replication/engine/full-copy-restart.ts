import { randomUUID } from 'node:crypto'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import {
  insertRolledBackChangesAfterHlc,
  insertRolledBackChangesNotFromPrimary,
  selectHistoryBase,
  selectNewestHistoryChange,
} from '../../core/system-catalog/index.js'
import { isGroupMember } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import type { SyncHistoryPoint } from '../types.js'
import { refreshCoordinatorState } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { selectSharedPointHlc } from './history-rollback.js'
import { initialSyncState, servesItsData } from './internal-types.js'
import { reportEngineError } from './report-error.js'
import { historyOrigins } from './term-history.js'

function asksThisNodeToRebuild(engine: ReplicationEngine, state: ReplicationGroupState | null): boolean {
  if (!state || state.currentPrimary?.nodeId === engine.nodeId) return false
  return state.repairingNodeIds.includes(engine.nodeId) && !state.faultedNodeIds.includes(engine.nodeId)
}

async function currentGroupState(engine: ReplicationEngine): Promise<ReplicationGroupState | null> {
  try {
    return await refreshCoordinatorState(engine)
  } catch (err: unknown) {
    reportEngineError(engine, err, 'repair-rebuild-confirmation', true)
    throw err
  }
}

/**
 * Returns whether a node that serves its data may start the full copy of a rebuild, reading the group's state again
 * first when the group's repairing flag started that rebuild. An operator who clears the flag before the copy starts
 * cancels the rebuild, so the node keeps its rows and serves them as before. When the coordinator cannot answer, the
 * node waits for a later read before it starts the copy.
 *
 * @param engine - The node's engine.
 * @returns True for a copy that the node's history requires, and for a rebuild whose flag the group still holds.
 *
 * @internal
 */
export async function rebuildStillRequested(engine: ReplicationEngine): Promise<boolean> {
  const rejoin = engine.rejoin
  if (!rejoin.rebuildingForRepairFlag) return true
  if (asksThisNodeToRebuild(engine, await currentGroupState(engine))) return true
  rejoin.rebuildingForRepairFlag = false
  rejoin.repairGeneration++
  engine.history.keptBeforeCopyTerm = null
  return false
}

async function selectHistoryTail(engine: ReplicationEngine): Promise<SyncHistoryPoint | null> {
  const conn = engine.writerConn
  const newest = await selectNewestHistoryChange(conn, CHANGES_TABLE, historyOrigins(engine))
  const base = await selectHistoryBase(conn)
  const tail = newest !== null && (base === null || newest.hlc > base.hlc) ? newest : base
  return tail === null ? null : { hlc: tail.hlc, txId: tail.txId }
}

/**
 * Asks the current primary where this replica's history meets the primary's, by sending a sync request that names the
 * replica's newest history transaction. A replica whose change log holds no history transaction has nothing to compare
 * or keep, so it asks for a full copy at once.
 *
 * @param engine - The replica's engine.
 * @param primaryId - The current primary's node ID.
 * @param term - The current primary term.
 * @param keepsBeforeCopy - True when the replica asks only to find which changes to keep before a full copy, and false
 * when it asks so that it can reconcile its history in place.
 * @returns A promise that settles once the replica has sent the request or started the full copy.
 *
 * @internal
 */
export async function requestHistoryPoint(
  engine: ReplicationEngine,
  primaryId: string,
  term: bigint,
  keepsBeforeCopy: boolean,
): Promise<void> {
  const history = engine.history
  const repairGeneration = engine.rejoin.repairGeneration
  const requestId = randomUUID()
  if (!keepsBeforeCopy) history.reconciling = true
  history.resumeRequest.start(requestId, term, keepsBeforeCopy)
  const tail = await selectHistoryTail(engine)
  if (!history.resumeRequest.matches(requestId)) return
  if (tail === null) {
    history.resumeRequest.clear()
    if (!(await rebuildStillRequested(engine)) || repairGeneration !== engine.rejoin.repairGeneration) return
    history.keptBeforeCopyTerm = term
    await startFullCopy(engine, repairGeneration)
    return
  }
  await engine.config.transport.requestSync(
    primaryId,
    engine.decorate({
      requestId,
      joinerNodeId: engine.nodeId,
      completedTables: [],
      supportsStreamVerification: true,
      historyTail: tail,
    }),
  )
}

/**
 * Keeps, in this node's table of rolled-back changes, every history change that the current primary can lack, ahead
 * of a full copy that replaces the node's rows. With a shared transaction that the node holds a record of, the node
 * keeps every change after it. Otherwise, because the primary no longer keeps the history to compare against, the
 * node keeps every history change that the primary did not write and marks them as not compared.
 *
 * @param engine - The node's engine.
 * @param point - The last transaction that the node shares with the primary, or undefined when the primary named none.
 * @param term - The primary term of the full copy that follows.
 * @param repairGeneration - The repair request under which the caller prepares the copy. The node keeps nothing once
 * an operator has cancelled that request.
 * @returns A promise that settles once the node has kept the changes.
 *
 * @internal
 */
export async function keepUnsharedChanges(
  engine: ReplicationEngine,
  point: SyncHistoryPoint | undefined,
  term: bigint,
  repairGeneration = engine.rejoin.repairGeneration,
): Promise<void> {
  const primaryId = engine.coordinatorState?.currentPrimary?.nodeId
  if (!engine.isCoordinatorMode() || !servesItsData(engine.syncState.phase)) return
  if (repairGeneration !== engine.rejoin.repairGeneration) return
  const origins = historyOrigins(engine)
  await engine.history.exclusive(() =>
    engine.writerConn.transaction(async tx => {
      if (repairGeneration !== engine.rejoin.repairGeneration) return
      const nowSeconds = Date.now() / 1000
      const cut = point ? await selectSharedPointHlc(tx, point) : null
      if (cut !== null) {
        await insertRolledBackChangesAfterHlc(tx, CHANGES_TABLE, origins, cut, term, nowSeconds)
      } else if (primaryId) {
        await insertRolledBackChangesNotFromPrimary(tx, CHANGES_TABLE, origins, primaryId, term, nowSeconds)
      }
    }),
  )
  if (repairGeneration === engine.rejoin.repairGeneration) engine.history.keptBeforeCopyTerm = term
}

/**
 * Makes a node that serves its data stop serving it and ask its source for a full copy, which replaces every row. A
 * node that is already copying keeps its copy going.
 *
 * @param engine - The node's engine.
 * @param repairGeneration - The repair request under which the caller prepared the copy.
 * @returns A promise that settles once the node has sent its sync request.
 *
 * @internal
 */
export async function startFullCopy(
  engine: ReplicationEngine,
  repairGeneration = engine.rejoin.repairGeneration,
): Promise<void> {
  if (!engine.running || repairGeneration !== engine.rejoin.repairGeneration) return
  if (!(await rebuildStillRequested(engine))) return
  if (!engine.running || repairGeneration !== engine.rejoin.repairGeneration) return
  const state = engine.coordinatorState
  if (engine.isCoordinatorMode()) {
    if (!state || !state.currentPrimary || !isGroupMember(state, engine.nodeId)) return
    if (state.currentPrimary.nodeId === engine.nodeId || state.faultedNodeIds.includes(engine.nodeId)) return
    if (engine.history.keptBeforeCopyTerm !== state.primaryTerm) return
  }
  if (!servesItsData(engine.syncState.phase)) return
  engine.history.resumeRequest.clear()
  engine.highestSourceSeqSeen = 0n
  engine.syncState = { ...initialSyncState(), phase: 'pending' }
  await engine.log.setSyncMeta('pending')
  await engine.syncJoiner.initiateSync()
}

/**
 * Rebuilds a node that serves its data through a full copy, after it keeps every change that the current primary can
 * lack. In coordinator mode the node first asks the primary where their histories meet, unless it has already kept its
 * changes in the current term, and the answer to that request starts the copy. A node that is already copying keeps
 * its copy going.
 *
 * @param engine - The node's engine.
 * @returns A promise that settles once the node has sent its request.
 *
 * @internal
 */
export async function restartWithFullCopy(engine: ReplicationEngine): Promise<void> {
  if (!servesItsData(engine.syncState.phase)) return
  const repairGeneration = engine.rejoin.repairGeneration
  const state = engine.coordinatorState
  const primaryId = state?.currentPrimary?.nodeId
  const history = engine.history
  if (
    !engine.isCoordinatorMode() ||
    !state ||
    !primaryId ||
    primaryId === engine.nodeId ||
    history.keptBeforeCopyTerm === state.primaryTerm
  ) {
    if (await rebuildStillRequested(engine)) await startFullCopy(engine, repairGeneration)
    return
  }
  if (history.resumeRequest.awaitsAnswerIn(state.primaryTerm, engine.syncAckTimeoutMs)) return
  await requestHistoryPoint(engine, primaryId, state.primaryTerm, true)
}
