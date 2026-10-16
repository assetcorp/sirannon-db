import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError } from '../errors.js'
import { DEFAULT_COORDINATOR_SESSION_TTL_MS } from './constants.js'
import { startControllerLoop, stopControllerLeaseWatch } from './controller-loop.js'
import {
  applyCoordinatorState,
  hasCurrentPrimaryAuthorityFor,
  noteCoordinatorContact,
} from './coordinator-authority.js'
import {
  admitRejoinedNodeIfReady,
  handleFormerPrimaryDemotion,
  reconcileInSyncSet,
  recordQuarantineIfFaulted,
  startCoordinatorRejoinSyncIfReady,
} from './coordinator-membership.js'
import type { ReplicationEngine } from './engine.js'
import { keepTermHistoryCurrent } from './history-anchor.js'
import { reportEngineError } from './report-error.js'
import { unrefTimer } from './timers.js'

const IN_SYNC_RECONCILE_INTERVAL_MS = 1_000

function localCoordinatorPrimary(engine: ReplicationEngine): { nodeId: string; endpoint?: string } {
  const endpoint = engine.config.coordinator?.endpoint
  return endpoint ? { nodeId: engine.nodeId, endpoint } : { nodeId: engine.nodeId }
}

export async function startCoordinatorMode(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return

  const coordinator = config.coordinator
  engine.coordinatorDemotionHandler = next => demoteFormerPrimary(engine, next)
  let state = await coordinator.getReplicationGroupState(config.clusterId, config.groupId)
  if (!state && config.votingDataBearingNodeIds) {
    state = await coordinator.setReplicationGroupState({
      clusterId: config.clusterId,
      groupId: config.groupId,
      votingDataBearingNodeIds: config.votingDataBearingNodeIds,
      currentPrimary: engine.config.topology.role === 'primary' ? localCoordinatorPrimary(engine) : null,
      primaryTerm: 1n,
      inSyncNodeIds: [engine.nodeId],
      compatibility: config.compatibility,
    })
  }
  if (!state) {
    throw new CoordinatorError(`Replication group '${config.groupId}' is not registered`)
  }
  noteCoordinatorContact(engine)
  engine.coordinatorState = state
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)

  const session = await coordinator.registerNodeSession({
    clusterId: config.clusterId,
    nodeId: engine.nodeId,
    ttlMs: config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS,
    endpoint: config.endpoint,
    groupIds: [config.groupId],
    dataBearing: true,
    voting: state.votingDataBearingNodeIds.includes(engine.nodeId),
    compatibility: config.compatibility,
  })
  engine.nodeSessionLeaseId = session.lease.id
  noteCoordinatorContact(engine)

  engine.coordinatorWatchDisposer = await coordinator.watchReplicationGroup(config.clusterId, config.groupId, next => {
    handleCoordinatorStateUpdate(engine, next)
  })

  if (coordinator.watchNodeSessions) {
    engine.nodeSessionWatchDisposer = await coordinator.watchNodeSessions(config.clusterId, liveNodeIds => {
      engine.liveNodeIds = [...liveNodeIds]
    })
  }

  startCoordinatorLeaseRenewal(engine)
  startInSyncReconcileLoop(engine)
  await startControllerLoop(engine)
}

function startInSyncReconcileLoop(engine: ReplicationEngine): void {
  const timer = setInterval(() => {
    keepTermHistoryCurrent(engine)
    reconcileInSyncSet(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'coordinator-in-sync-reconcile', true),
    )
    keepRejoinCurrent(engine)
  }, IN_SYNC_RECONCILE_INTERVAL_MS)
  unrefTimer(timer)
  engine.inSyncReconcileTimer = timer
}

function handleCoordinatorStateUpdate(engine: ReplicationEngine, next: ReplicationGroupState): void {
  recordQuarantine(engine, next)
  if (applyCoordinatorState(engine, next)) return
  keepTermHistoryCurrent(engine)
  keepRejoinCurrent(engine)
}

function recordQuarantine(engine: ReplicationEngine, state: ReplicationGroupState): void {
  if (!engine.running) return
  recordQuarantineIfFaulted(engine, state).catch((err: unknown) =>
    reportEngineError(engine, err, 'coordinator-quarantine-record', true),
  )
}

function keepRejoinCurrent(engine: ReplicationEngine): void {
  const state = engine.coordinatorState
  if (!state) return
  recordQuarantine(engine, state)
  startCoordinatorRejoinSyncIfReady(engine, state)
  admitRejoinedNodeIfReady(engine).catch((err: unknown) =>
    reportEngineError(engine, err, 'coordinator-rejoin-admission', true),
  )
}

function demoteFormerPrimary(engine: ReplicationEngine, next: ReplicationGroupState): void {
  if (!engine.running) return
  handleFormerPrimaryDemotion(engine, next).catch((err: unknown) =>
    reportEngineError(engine, err, 'coordinator-former-primary-demotion', false),
  )
}

function startCoordinatorLeaseRenewal(engine: ReplicationEngine): void {
  const config = engine.config.coordinator
  if (!config || !engine.nodeSessionLeaseId) return
  const ttlMs = config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
  const timer = setInterval(
    () => {
      void keepCoordinatorSessionAlive(engine, ttlMs)
    },
    Math.max(1_000, Math.floor(ttlMs / 3)),
  )
  unrefTimer(timer)
  engine.coordinatorLeaseTimer = timer
}

function applyAuthorityFromKnownState(engine: ReplicationEngine): void {
  const state = engine.coordinatorState
  engine.coordinatorAuthority = state ? hasCurrentPrimaryAuthorityFor(engine, state) : false
}

async function keepCoordinatorSessionAlive(engine: ReplicationEngine, ttlMs: number): Promise<void> {
  const config = engine.config.coordinator
  if (!config || !engine.running || engine.coordinatorSessionRestoring) return

  const leaseId = engine.nodeSessionLeaseId
  if (leaseId) {
    try {
      if (await config.coordinator.renewLease(leaseId, ttlMs)) {
        noteCoordinatorContact(engine)
        applyAuthorityFromKnownState(engine)
        return
      }
    } catch (err: unknown) {
      engine.coordinatorAuthority = false
      reportEngineError(engine, err, 'coordinator-session-renew', true)
      return
    }
  }

  engine.coordinatorAuthority = false
  engine.nodeSessionLeaseId = null
  await restoreCoordinatorSession(engine, ttlMs)
}

async function restoreCoordinatorSession(engine: ReplicationEngine, ttlMs: number): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return

  engine.coordinatorSessionRestoring = true
  try {
    const state = await config.coordinator.getReplicationGroupState(config.clusterId, config.groupId)
    if (!state) {
      throw new CoordinatorError(`Replication group '${config.groupId}' is not registered`)
    }
    const session = await config.coordinator.registerNodeSession({
      clusterId: config.clusterId,
      nodeId: engine.nodeId,
      ttlMs,
      endpoint: config.endpoint,
      groupIds: [config.groupId],
      dataBearing: true,
      voting: state.votingDataBearingNodeIds.includes(engine.nodeId),
      compatibility: config.compatibility,
    })
    if (!engine.running) {
      await config.coordinator.releaseLease(session.lease.id).catch(() => undefined)
      return
    }
    engine.nodeSessionLeaseId = session.lease.id
    applyCoordinatorState(engine, state)
  } catch (err: unknown) {
    reportEngineError(engine, err, 'coordinator-session-restore', true)
  } finally {
    engine.coordinatorSessionRestoring = false
  }
}

export function stopCoordinatorTimers(engine: ReplicationEngine): void {
  if (engine.coordinatorLeaseTimer) {
    clearInterval(engine.coordinatorLeaseTimer)
    engine.coordinatorLeaseTimer = null
  }
  if (engine.inSyncReconcileTimer) {
    clearInterval(engine.inSyncReconcileTimer)
    engine.inSyncReconcileTimer = null
  }
  if (engine.controller.timer) {
    clearInterval(engine.controller.timer)
    engine.controller.timer = null
  }
}

export async function stopCoordinatorMode(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  if (engine.coordinatorWatchDisposer) {
    await engine.coordinatorWatchDisposer()
    engine.coordinatorWatchDisposer = null
  }
  if (engine.nodeSessionWatchDisposer) {
    await engine.nodeSessionWatchDisposer()
    engine.nodeSessionWatchDisposer = null
  }
  engine.liveNodeIds = null
  await stopControllerLeaseWatch(engine)
  const leaseId = engine.controller.leaseId
  if (leaseId) {
    engine.controller.leaseId = null
    await config.coordinator
      .releaseLease(leaseId)
      .catch((err: unknown) => reportEngineError(engine, err, 'coordinator-controller-release', true))
  }
  await config.coordinator
    .deregisterNodeSession(config.clusterId, engine.nodeId)
    .catch((err: unknown) => reportEngineError(engine, err, 'coordinator-session-deregister', true))
}
