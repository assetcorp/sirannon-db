import { IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL, majorityOf } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError, StalePrimaryError } from '../errors.js'
import { DEFAULT_WRITE_CONCERN_TIMEOUT_MS } from './constants.js'
import { errorDetails, hasCurrentPrimaryAuthorityFor, refreshCoordinatorState } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { assertPrimaryInTerm, updateInSyncSetFromCurrent } from './in-sync-updates.js'
import { ensurePrimaryTermStart } from './term-history.js'

const WRITE_PROGRESS_PURPOSE = 'the progress of a write that it acknowledges'

/**
 * Returns the nodes whose copy every acknowledged write needs: this node, every node in the in-sync set, and every
 * node whose admission this node has requested and the coordinator has not answered, which includes a pending voter.
 *
 * @param engine - The primary's engine.
 * @param state - The group state that names the in-sync set.
 * @returns Those nodes, with this node first.
 *
 * @internal
 */
export function inSyncCopyHolderIds(engine: ReplicationEngine, state: ReplicationGroupState): string[] {
  return [...new Set([engine.nodeId, ...state.inSyncNodeIds, ...engine.inSync.pendingAdmissions])]
}

function holdsWrite(engine: ReplicationEngine, nodeId: string, seq: bigint): boolean {
  if (nodeId === engine.nodeId) return true
  const acked = engine.peerTracker.connectionAckedSeq(nodeId)
  return acked !== undefined && acked >= seq
}

function inSyncVotersHold(engine: ReplicationEngine, seq: bigint, state: ReplicationGroupState): boolean {
  const holders = inSyncCopyHolderIds(engine, state)
  const voters = state.votingDataBearingNodeIds
  const recordedVoters = new Set([engine.nodeId, ...state.inSyncNodeIds].filter(nodeId => voters.includes(nodeId)))
  if (recordedVoters.size < majorityOf(voters.length)) return false
  return holders.every(nodeId => holdsWrite(engine, nodeId, seq))
}

function eligibleVotersHold(engine: ReplicationEngine, seq: bigint, state: ReplicationGroupState): boolean {
  const eligible = state.votingDataBearingNodeIds.filter(nodeId => !state.drainingNodeIds.includes(nodeId))
  return [...eligible, ...engine.inSync.pendingAdmissions].every(nodeId => holdsWrite(engine, nodeId, seq))
}

function noteAcknowledged(engine: ReplicationEngine, seq: bigint): void {
  if (seq > engine.inSync.acknowledgedSeq) engine.inSync.acknowledgedSeq = seq
}

function stillPrimaryIn(engine: ReplicationEngine, term: bigint): ReplicationGroupState | null {
  const state = engine.coordinatorState
  if (!state || state.primaryTerm !== term || !hasCurrentPrimaryAuthorityFor(engine, state)) return null
  return state
}

async function waitForVoters(
  engine: ReplicationEngine,
  seq: bigint,
  level: 'majority' | 'all',
  term: bigint,
  deadline: number,
): Promise<void> {
  const holdsAt = level === 'majority' ? inSyncVotersHold : eligibleVotersHold
  const description = level === 'majority' ? 'every in-sync voter' : 'every voter'
  await engine.peerTracker.waitUntil(
    () => {
      const state = stillPrimaryIn(engine, term)
      if (!state) return true
      if (!holdsAt(engine, seq, state)) return false
      noteAcknowledged(engine, seq)
      return true
    },
    Math.max(0, deadline - Date.now()),
    `Timed out waiting for ${description} of the replication group to ACK seq ${seq}`,
  )
}

async function raiseDurabilityPointForWrite(engine: ReplicationEngine, seq: bigint, term: bigint): Promise<void> {
  await updateInSyncSetFromCurrent(engine, {
    purpose: WRITE_PROGRESS_PURPOSE,
    known: engine.coordinatorState ?? undefined,
    primaryTerm: term,
    change: base =>
      base.durabilityPointSeq >= seq ? null : { inSyncNodeIds: base.inSyncNodeIds, durabilityPointSeq: seq },
  })
}

/**
 * Returns once a write meets its write concern. In coordinator mode, a `majority` write waits until every in-sync
 * voter holds it and those voters are a majority of the voters, counting the voters in the newest group state that
 * this node holds, and an `all` write waits for every voter that is not draining. At feature level 1, this node also
 * raises the group's durability point to the write's position before it acknowledges the write.
 *
 * @param engine - The engine of the node that accepted the write.
 * @param seq - The write's position in this node's change log.
 * @param wc - The write concern, whose `timeoutMs` bounds the whole wait and defaults to 5000.
 * @throws WriteConcernError when the voters do not acknowledge the write in time.
 * @throws StalePrimaryError when this node loses primary duty, or never recorded the start of its term.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function waitForWriteConcern(
  engine: ReplicationEngine,
  seq: bigint,
  wc: { level: string; timeoutMs?: number },
): Promise<void> {
  const timeoutMs = wc.timeoutMs ?? DEFAULT_WRITE_CONCERN_TIMEOUT_MS

  if (!engine.isCoordinatorMode()) {
    if (wc.level === 'majority') {
      await engine.peerTracker.waitForMajority(seq, timeoutMs)
    } else if (wc.level === 'all') {
      await engine.peerTracker.waitForAll(seq, timeoutMs)
    }
    return
  }

  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!state) {
    throw new CoordinatorError('Cannot satisfy write concern without replication-group state')
  }
  if (engine.history.termStartTerm !== state.primaryTerm) await ensurePrimaryTermStart(engine)
  const level = wc.level
  if (level !== 'majority' && level !== 'all') return
  const term = state.primaryTerm
  assertPrimaryInTerm(engine, state, term, WRITE_PROGRESS_PURPOSE)
  if (engine.history.termStartTerm !== term) {
    throw new StalePrimaryError(
      `This node has not recorded the start of term ${term}, so it cannot prove its authority for the write`,
      errorDetails(engine, state),
    )
  }

  const deadline = Date.now() + timeoutMs
  const holdsAt = level === 'majority' ? inSyncVotersHold : eligibleVotersHold
  for (;;) {
    await waitForVoters(engine, seq, level, term, deadline)
    const current = engine.coordinatorState ?? state
    assertPrimaryInTerm(engine, current, term, WRITE_PROGRESS_PURPOSE)
    if (current.featureLevel >= IN_SYNC_ACKNOWLEDGEMENT_FEATURE_LEVEL) return
    await raiseDurabilityPointForWrite(engine, seq, term)
    const recorded = engine.coordinatorState ?? current
    assertPrimaryInTerm(engine, recorded, term, WRITE_PROGRESS_PURPOSE)
    if (holdsAt(engine, seq, recorded)) return
  }
}
