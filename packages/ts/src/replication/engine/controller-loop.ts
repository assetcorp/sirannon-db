import { compatibilityAllowsPromotion } from '../coordinator/compatibility.js'
import { applyCoordinatorState, noteCoordinatorContact, refreshCoordinatorState } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { unrefTimer } from './timers.js'

const DEFAULT_CONTROLLER_LEASE_TTL_MS = 10_000
const DEFAULT_CONTROLLER_TICK_INTERVAL_MS = 1_000

interface ControllerBid {
  holderId: string
  ttlMs: number
}

export async function startControllerLoop(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const controllerConfig = typeof config.controller === 'object' ? config.controller : {}
  const enabled = typeof config.controller === 'boolean' ? config.controller : (controllerConfig.enabled ?? true)
  if (!enabled) {
    engine.controllerState = 'disabled'
    return
  }

  engine.controllerState = 'standby'
  const bid: ControllerBid = {
    holderId: controllerConfig.holderId ?? engine.nodeId,
    ttlMs: controllerConfig.leaseTtlMs ?? DEFAULT_CONTROLLER_LEASE_TTL_MS,
  }

  const coordinator = config.coordinator
  if (coordinator.watchControllerLease) {
    engine.controller.leaseWatchDisposer = await coordinator.watchControllerLease(config.clusterId, lease => {
      engine.controller.observedLease = lease
      engine.controller.observedLeaseLiveUntilMs = lease === null ? 0 : Date.now() + lease.ttlMs
      if (lease === null) {
        bidForControllerLease(engine, bid).catch((err: unknown) => reportControllerFailure(engine, err))
      }
    })
  }

  const timer = setInterval(() => {
    controllerTick(engine, bid).catch((err: unknown) => reportControllerFailure(engine, err))
  }, controllerConfig.tickIntervalMs ?? DEFAULT_CONTROLLER_TICK_INTERVAL_MS)
  unrefTimer(timer)
  engine.controller.timer = timer
}

export async function stopControllerLeaseWatch(engine: ReplicationEngine): Promise<void> {
  const dispose = engine.controller.leaseWatchDisposer
  engine.controller.leaseWatchDisposer = null
  engine.controller.observedLease = null
  engine.controller.observedLeaseLiveUntilMs = 0
  if (dispose) {
    await dispose()
  }
}

async function controllerTick(engine: ReplicationEngine, bid: ControllerBid): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  if (engine.controller.leaseId) {
    const renewed = await config.coordinator.renewLease(engine.controller.leaseId, bid.ttlMs)
    if (!renewed) {
      engine.controller.leaseId = null
      engine.controllerState = 'lost'
      return
    }
    noteCoordinatorContact(engine)
    engine.controllerState = 'active'
    await runControllerPromotionCheck(engine)
    return
  }

  if (watchShowsLiveHolder(engine)) {
    engine.controllerState = 'standby'
    return
  }
  await bidForControllerLease(engine, bid)
}

function watchShowsLiveHolder(engine: ReplicationEngine): boolean {
  return (
    engine.controller.leaseWatchDisposer !== null &&
    engine.controller.observedLease !== null &&
    engine.controller.observedLeaseLiveUntilMs > Date.now()
  )
}

async function bidForControllerLease(engine: ReplicationEngine, bid: ControllerBid): Promise<void> {
  const config = engine.config.coordinator
  if (!config || !engine.running || engine.controller.leaseId || engine.controller.bidding) return

  engine.controller.bidding = true
  try {
    const acquired = await config.coordinator.tryAcquireControllerLease({
      clusterId: config.clusterId,
      holderId: bid.holderId,
      ttlMs: bid.ttlMs,
    })
    noteCoordinatorContact(engine)
    if (!acquired.acquired) {
      engine.controllerState = 'standby'
      return
    }
    if (!engine.running) {
      await config.coordinator.releaseLease(acquired.lease.id)
      return
    }
    engine.controller.leaseId = acquired.lease.id
    engine.controllerState = 'active'
  } finally {
    engine.controller.bidding = false
  }
  await runControllerPromotionCheck(engine)
}

function reportControllerFailure(engine: ReplicationEngine, err: unknown): void {
  const wrappedErr = err instanceof Error ? err : new Error(String(err))
  engine.controllerState = 'lost'
  engine.emitError({ error: wrappedErr, operation: 'coordinator-controller', recoverable: true })
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
    applyCoordinatorState(engine, promoted)
  } catch (err: unknown) {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'coordinator-promotion', recoverable: true })
  }
}
