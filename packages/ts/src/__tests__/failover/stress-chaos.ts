import { setTimeout as delay } from 'node:timers/promises'
import { pauseContainer, unpauseContainer } from './docker.js'
import { waitForGroup, waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  etcdProxyName,
  type GateEnvironment,
  grpcProxyName,
  killNode,
  type NodeOptions,
  setReplicationLinks,
  startNode,
} from './gate-environment.js'
import { waitForNodeReady } from './gate-observations.js'
import type { ChaosWindow } from './stress-load.js'

const HEAL_TIMEOUT_MS = 120_000
const FAILOVER_TIMEOUT_MS = 60_000
const REJOIN_TIMEOUT_MS = 120_000
const WRITE_SETTLE_GRACE_MS = 6_000
const REPLICA_STALL_MS = 5_000
const ETCD_MEMBER_FREEZE_MS = 10_000
const LINK_LATENCY_MS = 3
const LINK_LATENCY_TOXIC = 'stress-latency'
const ETCD_MEMBER_COUNT = 3

export const PRODUCTION_TIMINGS: NodeOptions = {
  sessionTtlMs: 10_000,
  controllerLeaseTtlMs: 10_000,
  controllerTickIntervalMs: 1_000,
}

export interface ChaosContext {
  environment: GateEnvironment
  members: readonly string[]
  windows: ChaosWindow[]
  random: () => number
}

export type ChaosAction = (context: ChaosContext) => Promise<void>

export function seededRandom(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296
  }
}

export async function addLinkLatency(environment: GateEnvironment): Promise<void> {
  for (const nodeId of environment.nodeIds) {
    for (let index = 0; index < ETCD_MEMBER_COUNT; index++) {
      await environment.toxiproxy.addLatency(etcdProxyName(nodeId, index), LINK_LATENCY_TOXIC, LINK_LATENCY_MS)
    }
    for (const peerId of environment.nodeIds) {
      if (peerId === nodeId) continue
      await environment.toxiproxy.addLatency(grpcProxyName(nodeId, peerId), LINK_LATENCY_TOXIC, LINK_LATENCY_MS)
    }
  }
}

async function currentPrimaryId(context: ChaosContext): Promise<string> {
  return (await waitForPrimary(context.environment, context.members, FAILOVER_TIMEOUT_MS)).currentPrimary.nodeId
}

async function replicaIds(context: ChaosContext): Promise<string[]> {
  const primaryId = await currentPrimaryId(context)
  return context.members.filter(nodeId => nodeId !== primaryId)
}

function pick<T>(context: ChaosContext, values: readonly T[]): T {
  const value = values[Math.floor(context.random() * values.length)]
  if (value === undefined) throw new Error('Nothing to pick from')
  return value
}

async function rejoin(context: ChaosContext, nodeId: string): Promise<void> {
  const node = await startNode(context.environment, nodeId, {
    role: 'replica',
    ...PRODUCTION_TIMINGS,
    awaitReady: false,
  })
  await waitForGroup(
    context.environment,
    group => group.inSyncNodeIds.includes(nodeId),
    REJOIN_TIMEOUT_MS,
    `${nodeId} back in the in-sync set`,
  )
  await waitForNodeReady(node, REJOIN_TIMEOUT_MS)
}

export async function runChaos(context: ChaosContext, name: string, action: ChaosAction): Promise<void> {
  const startMs = Date.now()
  try {
    await action(context)
    await waitForInSyncSet(context.environment, context.members, HEAL_TIMEOUT_MS)
  } finally {
    context.windows.push({ action: name, startMs, endMs: Date.now() + WRITE_SETTLE_GRACE_MS })
  }
}

export const failOverFromKilledPrimary: ChaosAction = async context => {
  const before = await waitForPrimary(context.environment, context.members, FAILOVER_TIMEOUT_MS)
  const primaryId = before.currentPrimary.nodeId
  await killNode(context.environment, primaryId)
  await waitForGroup(
    context.environment,
    group =>
      group.currentPrimary !== null &&
      group.currentPrimary.nodeId !== primaryId &&
      group.primaryTerm > before.primaryTerm,
    FAILOVER_TIMEOUT_MS,
    `a new primary after ${primaryId} was killed`,
  )
  await rejoin(context, primaryId)
}

export const restartPrimaryInPlace: ChaosAction = async context => {
  const primaryId = await currentPrimaryId(context)
  await killNode(context.environment, primaryId)
  await rejoin(context, primaryId)
}

export const restartReplica: ChaosAction = async context => {
  const replicaId = pick(context, await replicaIds(context))
  await killNode(context.environment, replicaId)
  await rejoin(context, replicaId)
}

export const stallReplica: ChaosAction = async context => {
  const replicaId = pick(context, await replicaIds(context))
  await setReplicationLinks(context.environment, replicaId, false)
  await delay(REPLICA_STALL_MS)
  await setReplicationLinks(context.environment, replicaId, true)
}

export const stallTwoReplicas: ChaosAction = async context => {
  const replicas = await replicaIds(context)
  const first = pick(context, replicas)
  const second = pick(
    context,
    replicas.filter(nodeId => nodeId !== first),
  )
  await setReplicationLinks(context.environment, first, false)
  await setReplicationLinks(context.environment, second, false)
  await delay(REPLICA_STALL_MS)
  await setReplicationLinks(context.environment, first, true)
  await setReplicationLinks(context.environment, second, true)
}

export const freezeEtcdMember: ChaosAction = async context => {
  const containerName = pick(context, context.environment.etcd.containerNames)
  await pauseContainer(containerName)
  try {
    await delay(ETCD_MEMBER_FREEZE_MS)
  } finally {
    await unpauseContainer(containerName)
  }
}

export const restartEveryNodeInPlace: ChaosAction = async context => {
  const primaryId = await currentPrimaryId(context)
  const [firstReplicaId, ...otherReplicaIds] = context.members.filter(nodeId => nodeId !== primaryId)
  const order = firstReplicaId === undefined ? [primaryId] : [firstReplicaId, primaryId, ...otherReplicaIds]
  for (const nodeId of order) {
    await killNode(context.environment, nodeId)
    await rejoin(context, nodeId)
    await waitForInSyncSet(context.environment, context.members, HEAL_TIMEOUT_MS)
  }
}

export const rollingRestart: ChaosAction = async context => {
  for (const replicaId of await replicaIds(context)) {
    await killNode(context.environment, replicaId)
    await rejoin(context, replicaId)
    await waitForInSyncSet(context.environment, context.members, HEAL_TIMEOUT_MS)
  }
  await failOverFromKilledPrimary(context)
}
