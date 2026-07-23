import { compatibilityAllowsPromotion } from '../coordinator/compatibility.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError } from '../errors.js'
import { hasCurrentPrimaryAuthorityFor, refreshCoordinatorState } from './coordinator-authority.js'
import { handleFormerPrimaryDemotion, startCoordinatorRejoinSyncIfReady } from './coordinator-membership.js'
import type { ReplicationEngine } from './engine.js'

export const DEFAULT_COORDINATOR_SESSION_TTL_MS = 10_000
const DEFAULT_CONTROLLER_LEASE_TTL_MS = 10_000
const DEFAULT_CONTROLLER_TICK_INTERVAL_MS = 1_000

function unrefTimer(timer: ReturnType<typeof setInterval>): void {
  const unref = (timer as { unref?: () => void }).unref
  unref?.call(timer)
}

function localCoordinatorPrimary(engine: ReplicationEngine): { nodeId: string; endpoint?: string } {
  const endpoint = engine.config.coordinator?.endpoint
  return endpoint ? { nodeId: engine.nodeId, endpoint } : { nodeId: engine.nodeId }
}

export async function startCoordinatorMode(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return

  const coordinator = config.coordinator
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

  engine.coordinatorWatchDisposer = await coordinator.watchReplicationGroup(config.clusterId, config.groupId, next => {
    handleCoordinatorStateUpdate(engine, next)
  })

  startCoordinatorLeaseRenewal(engine)
  startControllerLoop(engine)
}

function handleCoordinatorStateUpdate(engine: ReplicationEngine, next: ReplicationGroupState): void {
  const previous = engine.coordinatorState
  const wasPrimary = previous ? hasCurrentPrimaryAuthorityFor(engine, previous) : engine.coordinatorAuthority
  engine.coordinatorState = next
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, next)

  if (wasPrimary && !engine.coordinatorAuthority && next.primaryTerm > (previous?.primaryTerm ?? 0n)) {
    handleFormerPrimaryDemotion(engine, next).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'coordinator-former-primary-demotion', recoverable: false })
    })
    return
  }

  startCoordinatorRejoinSyncIfReady(engine, next)
}

function startCoordinatorLeaseRenewal(engine: ReplicationEngine): void {
  const config = engine.config.coordinator
  const leaseId = engine.nodeSessionLeaseId
  if (!config || !leaseId) return
  const ttlMs = config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
  const timer = setInterval(
    () => {
      config.coordinator
        .renewLease(leaseId, ttlMs)
        .then(renewed => {
          if (!renewed) {
            engine.coordinatorAuthority = false
          }
        })
        .catch((err: unknown) => {
          engine.coordinatorAuthority = false
          const wrappedErr = err instanceof Error ? err : new Error(String(err))
          engine.emitError({ error: wrappedErr, operation: 'coordinator-session-renew', recoverable: false })
        })
    },
    Math.max(1_000, Math.floor(ttlMs / 3)),
  )
  unrefTimer(timer)
  engine.coordinatorLeaseTimer = timer
}

function startControllerLoop(engine: ReplicationEngine): void {
  const config = engine.config.coordinator
  if (!config) return
  const controllerConfig = typeof config.controller === 'object' ? config.controller : {}
  const enabled = typeof config.controller === 'boolean' ? config.controller : (controllerConfig.enabled ?? true)
  if (!enabled) {
    engine.controllerState = 'disabled'
    return
  }

  engine.controllerState = 'standby'
  const ttlMs = controllerConfig.leaseTtlMs ?? DEFAULT_CONTROLLER_LEASE_TTL_MS
  const holderId = controllerConfig.holderId ?? engine.nodeId
  const tickMs = controllerConfig.tickIntervalMs ?? DEFAULT_CONTROLLER_TICK_INTERVAL_MS
  const timer = setInterval(() => {
    controllerTick(engine, holderId, ttlMs).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.controllerState = 'lost'
      engine.emitError({ error: wrappedErr, operation: 'coordinator-controller', recoverable: true })
    })
  }, tickMs)
  unrefTimer(timer)
  engine.controllerTimer = timer
}

async function controllerTick(engine: ReplicationEngine, holderId: string, ttlMs: number): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  if (engine.controllerLeaseId) {
    const renewed = await config.coordinator.renewLease(engine.controllerLeaseId, ttlMs)
    if (!renewed) {
      engine.controllerLeaseId = null
      engine.controllerState = 'lost'
      return
    }
    engine.controllerState = 'active'
    await runControllerPromotionCheck(engine)
    return
  }

  const acquired = await config.coordinator.tryAcquireControllerLease({
    clusterId: config.clusterId,
    holderId,
    ttlMs,
  })
  if (acquired.acquired) {
    engine.controllerLeaseId = acquired.lease.id
    engine.controllerState = 'active'
    await runControllerPromotionCheck(engine)
  } else {
    engine.controllerState = 'standby'
  }
}

async function runControllerPromotionCheck(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const state = await refreshCoordinatorState(engine)
  if (!state) return
  const primaryNodeId = state.currentPrimary?.nodeId
  const primaryLive = primaryNodeId
    ? await config.coordinator.getLiveNodeSession(config.clusterId, primaryNodeId)
    : null
  const primaryCanKeepDuty =
    primaryNodeId &&
    primaryLive &&
    compatibilityAllowsPromotion(state.compatibility, primaryLive.compatibility) &&
    !state.drainingNodeIds.includes(primaryNodeId) &&
    !state.repairingNodeIds.includes(primaryNodeId) &&
    !state.faultedNodeIds.includes(primaryNodeId)
  if (primaryCanKeepDuty) {
    return
  }
  try {
    const promoted = await config.coordinator.promoteEligibleReplica({
      clusterId: config.clusterId,
      groupId: config.groupId,
      excludeNodeIds: primaryNodeId ? [primaryNodeId] : [],
    })
    engine.coordinatorState = promoted
    engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, promoted)
  } catch (err: unknown) {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'coordinator-promotion', recoverable: true })
  }
}

export function stopCoordinatorTimers(engine: ReplicationEngine): void {
  if (engine.coordinatorLeaseTimer) {
    clearInterval(engine.coordinatorLeaseTimer)
    engine.coordinatorLeaseTimer = null
  }
  if (engine.controllerTimer) {
    clearInterval(engine.controllerTimer)
    engine.controllerTimer = null
  }
}

export async function stopCoordinatorMode(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  if (engine.coordinatorWatchDisposer) {
    await engine.coordinatorWatchDisposer()
    engine.coordinatorWatchDisposer = null
  }
  if (engine.controllerLeaseId) {
    const leaseId = engine.controllerLeaseId
    engine.controllerLeaseId = null
    await config.coordinator.releaseLease(leaseId).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'coordinator-controller-release', recoverable: true })
    })
  }
  await config.coordinator.deregisterNodeSession(config.clusterId, engine.nodeId).catch((err: unknown) => {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'coordinator-session-deregister', recoverable: true })
  })
}
