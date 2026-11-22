import type { RegisterNodeSessionInput, ReplicationGroupState } from '../coordinator/types.js'
import { AuthorityError, CoordinatorError, ProtocolVersionMismatchError, TopologyError } from '../errors.js'
import type { CoordinatorModeConfig, ReplicationConfig } from '../types.js'
import { reportPositionForAdmission } from './admission-report.js'
import {
  COORDINATOR_ENDPOINTS_REFUSAL,
  COORDINATOR_SESSION_RENEWAL_FLOOR_MS,
  DEFAULT_COORDINATOR_SESSION_TTL_MS,
  FIRST_PRIMARY_TERM,
  REPLICA_LAG_CHECK_INTERVAL_MS,
  SUPPORTED_FEATURE_LEVEL,
} from './constants.js'
import { startControllerLoop, stopControllerLeaseWatch } from './controller-loop.js'
import {
  applyCoordinatorState,
  errorDetails,
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
import { removeLaggingReplicas } from './in-sync-lag.js'
import { applyCoordinatorRestartRules } from './local-membership.js'
import { refreshMemberAddresses } from './member-addresses.js'
import { reportEngineError } from './report-error.js'
import { beginRestartProof, checkRestartProof } from './restart-proof.js'
import {
  ensurePrimaryTermStart,
  forgetUnheldFirstPrimaryTermStart,
  recordFirstPrimaryTermStart,
} from './term-history.js'
import { unrefTimer } from './timers.js'

const IN_SYNC_RECONCILE_INTERVAL_MS = 1_000
const MEMBER_ADDRESS_REFRESH_INTERVAL_MS = 5_000

function localCoordinatorPrimary(engine: ReplicationEngine): { nodeId: string; endpoint?: string } {
  const endpoint = engine.config.coordinator?.endpoint
  return endpoint ? { nodeId: engine.nodeId, endpoint } : { nodeId: engine.nodeId }
}

/**
 * Refuses a replication config that coordinator mode cannot run with: one without a stable node ID, and one that
 * configures peer addresses, which coordinator mode takes from the members' sessions, one with a replica lag limit
 * that is not a positive whole number of milliseconds, and one with a session lease of 1,000 ms or less, which lapses
 * before the node's next renewal.
 *
 * @param config - The replication config that the engine receives.
 * @throws AuthorityError when coordinator mode has no `nodeId`.
 * @throws TopologyError when coordinator mode has `transportConfig.endpoints`.
 * @throws RangeError when `coordinator.maxReplicaLagMs` is not a positive safe integer, or when
 * `coordinator.sessionTtlMs` is not a safe integer above 1,000.
 *
 * @internal
 */
export function assertCoordinatorModeConfig(config: ReplicationConfig): void {
  if (!config.coordinator) return
  if (!config.nodeId) {
    throw new AuthorityError('Coordinator mode requires a stable persisted nodeId')
  }
  const lagMs = config.coordinator.maxReplicaLagMs
  if (lagMs !== undefined && (!Number.isSafeInteger(lagMs) || lagMs <= 0)) {
    throw new RangeError('coordinator.maxReplicaLagMs must be a positive safe integer')
  }
  const sessionTtlMs = config.coordinator.sessionTtlMs
  if (
    sessionTtlMs !== undefined &&
    (!Number.isSafeInteger(sessionTtlMs) || sessionTtlMs <= COORDINATOR_SESSION_RENEWAL_FLOOR_MS)
  ) {
    throw new RangeError(
      `coordinator.sessionTtlMs must be a safe integer above ${COORDINATOR_SESSION_RENEWAL_FLOOR_MS}, because a node renews its session at most once every ${COORDINATOR_SESSION_RENEWAL_FLOOR_MS} ms, so a shorter lease lapses between renewals`,
    )
  }
  if (config.transportConfig?.endpoints !== undefined) {
    throw new TopologyError(COORDINATOR_ENDPOINTS_REFUSAL)
  }
}

function assertSupportedFeatureLevel(engine: ReplicationEngine, state: ReplicationGroupState): void {
  if (state.featureLevel <= SUPPORTED_FEATURE_LEVEL) return
  throw new ProtocolVersionMismatchError(
    `Replication group '${state.groupId}' runs at feature level ${state.featureLevel}, and this node supports levels up to ${SUPPORTED_FEATURE_LEVEL}`,
    {
      ...errorDetails(engine, state),
      groupFeatureLevel: state.featureLevel,
      supportedFeatureLevel: SUPPORTED_FEATURE_LEVEL,
    },
  )
}

function sessionRegistration(
  engine: ReplicationEngine,
  config: CoordinatorModeConfig,
  state: ReplicationGroupState,
  ttlMs: number,
): RegisterNodeSessionInput {
  return {
    clusterId: config.clusterId,
    nodeId: engine.nodeId,
    ttlMs,
    endpoint: config.endpoint,
    replicationEndpoint: config.replicationEndpoint,
    groupIds: [config.groupId],
    dataBearing: true,
    voting: state.votingDataBearingNodeIds.includes(engine.nodeId),
    compatibility: config.compatibility,
    featureLevel: SUPPORTED_FEATURE_LEVEL,
  }
}

async function openReplicationGroup(
  engine: ReplicationEngine,
  config: CoordinatorModeConfig,
): Promise<{ state: ReplicationGroupState; created: boolean }> {
  const coordinator = config.coordinator
  const existing = await coordinator.getReplicationGroupState(config.clusterId, config.groupId)
  if (existing) {
    await forgetUnheldFirstPrimaryTermStart(engine, existing)
    return { state: existing, created: false }
  }
  if (config.votingDataBearingNodeIds) {
    const createsAsPrimary = engine.config.topology.role === 'primary'
    if (createsAsPrimary) await recordFirstPrimaryTermStart(engine, config.votingDataBearingNodeIds)
    const opened = await coordinator.createReplicationGroupState({
      clusterId: config.clusterId,
      groupId: config.groupId,
      votingDataBearingNodeIds: config.votingDataBearingNodeIds,
      currentPrimary: createsAsPrimary ? localCoordinatorPrimary(engine) : null,
      primaryTerm: FIRST_PRIMARY_TERM,
      inSyncNodeIds: [engine.nodeId],
      compatibility: config.compatibility,
      featureLevel: SUPPORTED_FEATURE_LEVEL,
    })
    if (opened.state) {
      if (!opened.created) await forgetUnheldFirstPrimaryTermStart(engine, opened.state)
      return { state: opened.state, created: opened.created }
    }
  }
  throw new CoordinatorError(`Replication group '${config.groupId}' is not registered`)
}

export async function startCoordinatorMode(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return

  const coordinator = config.coordinator
  engine.coordinatorDemotionHandler = next => demoteFormerPrimary(engine, next)
  const opened = await openReplicationGroup(engine, config)
  assertSupportedFeatureLevel(engine, opened.state)
  noteCoordinatorContact(engine)
  engine.coordinatorState = opened.state
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, opened.state)
  if (!opened.created) await beginRestartProof(engine, await applyCoordinatorRestartRules(engine, opened.state))
  const state = engine.coordinatorState ?? opened.state
  engine.memberAddresses.noteGroupState(state)
  if (opened.created) {
    await ensurePrimaryTermStart(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'primary-term-start', true),
    )
  }

  const registration = sessionRegistration(engine, config, state, sessionTtlMs(config))
  const session = await coordinator.registerNodeSession(registration)
  engine.nodeSessionLeaseId = session.lease.id
  engine.nodeSessionVoting = registration.voting ?? null
  noteCoordinatorContact(engine)

  engine.coordinatorWatchDisposer = await coordinator.watchReplicationGroup(config.clusterId, config.groupId, next => {
    handleCoordinatorStateUpdate(engine, next)
  })

  if (coordinator.watchNodeSessions) {
    engine.nodeSessionWatchDisposer = await coordinator.watchNodeSessions(config.clusterId, liveNodeIds => {
      engine.liveNodeIds = [...liveNodeIds]
      engine.memberAddresses.markStale()
    })
  }

  startCoordinatorLeaseRenewal(engine)
  startInSyncReconcileLoop(engine)
  await startControllerLoop(engine)
}

function sessionTtlMs(config: CoordinatorModeConfig): number {
  return config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
}

function startInSyncReconcileLoop(engine: ReplicationEngine): void {
  const timer = setInterval(() => {
    keepTermHistoryCurrent(engine)
    checkRestartProof(engine).catch((err: unknown) => reportEngineError(engine, err, 'restart-proof-check', true))
    reconcileInSyncSet(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'coordinator-in-sync-reconcile', true),
    )
    reportPositionForAdmission(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'coordinator-admission-report', true),
    )
    keepRejoinCurrent(engine)
    keepMemberSessionsCurrent(engine)
  }, IN_SYNC_RECONCILE_INTERVAL_MS)
  unrefTimer(timer)
  engine.inSync.reconcileTimer = timer
  const lagTimer = setInterval(() => {
    removeLaggingReplicas(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'coordinator-lagging-replica-removal', true),
    )
  }, REPLICA_LAG_CHECK_INTERVAL_MS)
  unrefTimer(lagTimer)
  engine.inSync.lagTimer = lagTimer
}

function keepMemberSessionsCurrent(engine: ReplicationEngine): void {
  const config = engine.config.coordinator
  const state = engine.coordinatorState
  if (!config || !state || !engine.running) return
  const addresses = engine.memberAddresses
  const refreshDue = Date.now() - addresses.refreshedAtMs >= MEMBER_ADDRESS_REFRESH_INTERVAL_MS
  if (addresses.stale || refreshDue || !config.coordinator.watchNodeSessions) {
    refreshMemberAddresses(engine).catch((err: unknown) =>
      reportEngineError(engine, err, 'coordinator-member-addresses', true),
    )
  }
  const sessionVoting = engine.nodeSessionVoting
  const voting = state.votingDataBearingNodeIds.includes(engine.nodeId)
  if (sessionVoting === null || sessionVoting === voting || engine.coordinatorSessionRestoring) return
  void restoreCoordinatorSession(engine, sessionTtlMs(config))
}

function handleCoordinatorStateUpdate(engine: ReplicationEngine, next: ReplicationGroupState): void {
  recordQuarantine(engine, next)
  engine.memberAddresses.noteGroupState(next)
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
    Math.max(COORDINATOR_SESSION_RENEWAL_FLOOR_MS, Math.floor(ttlMs / 3)),
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
    const registration = sessionRegistration(engine, config, state, ttlMs)
    const session = await config.coordinator.registerNodeSession(registration)
    if (!engine.running) {
      await config.coordinator.releaseLease(session.lease.id).catch(() => undefined)
      return
    }
    engine.nodeSessionLeaseId = session.lease.id
    engine.nodeSessionVoting = registration.voting ?? null
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
  if (engine.inSync.reconcileTimer) {
    clearInterval(engine.inSync.reconcileTimer)
    engine.inSync.reconcileTimer = null
  }
  if (engine.inSync.lagTimer) {
    clearInterval(engine.inSync.lagTimer)
    engine.inSync.lagTimer = null
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
