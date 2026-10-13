import { type GateEnvironment, GROUP_ID, serverBaseUrlFor } from './gate-environment.js'
import { describeValue, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess, SerializedError } from './node-process.js'

export interface FailoverStatus {
  nodeId: string
  role: string
  peers: Array<{ nodeId: string; lastAckedSeq: string; connected: boolean }>
  localSeq: string
  health: { state: string; reason: string; canRead: boolean; canWrite: boolean }
  syncState?: {
    phase: string
    sourcePeerId: string | null
  }
  coordinator?: {
    currentPrimary: { nodeId: string; endpoint?: string } | null
    primaryTerm: string
    inSyncNodeIds: string[]
    authority: boolean
    connected: boolean
    controllerState: 'disabled' | 'standby' | 'active' | 'lost'
  }
  recentErrors?: Array<{
    error?: SerializedError
    operation?: string
    peerId?: string
    recoverable?: boolean
  }>
}

export interface ReadinessResponse {
  status: string
  replication?: {
    role?: string
    primaryTerm?: string
    currentPrimary?: string
    readAvailability?: 'available' | 'unavailable'
    writeAvailability?: 'available' | 'unavailable'
    syncState?: string
    healthReason?: string
    inSyncReplicas?: string[]
    laggingReplicas?: string[]
    coordinator?: {
      connected: boolean
      authority: boolean
    }
    controller?: {
      state: string
    }
  }
}

export interface ClusterStatusResponse {
  databaseId: string
  replicationGroupId?: string
  role: string
  currentPrimary: { nodeId: string; endpoint?: string } | null
  primaryTerm?: string
  readEndpoints?: Array<{ nodeId: string; endpoint: string; readConcerns: string[] }>
  health: string
  healthReason: string
}

export async function statusOf(node: FailoverNodeProcess): Promise<FailoverStatus> {
  return (await node.status()) as unknown as FailoverStatus
}

export async function readinessOf(environment: GateEnvironment, nodeId: string): Promise<ReadinessResponse> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/health/ready`, {
    signal: AbortSignal.timeout(5_000),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Readiness for ${nodeId} returned ${response.status}: ${text}`)
  }
  return JSON.parse(text) as ReadinessResponse
}

export async function clusterStatusOf(environment: GateEnvironment, nodeId: string): Promise<ClusterStatusResponse> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/cluster`, {
    signal: AbortSignal.timeout(5_000),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Cluster status for ${nodeId} returned ${response.status}: ${text}`)
  }
  return JSON.parse(text) as ClusterStatusResponse
}

export async function readableNodeIds(environment: GateEnvironment, nodeId: string): Promise<string[]> {
  const status = await clusterStatusOf(environment, nodeId)
  return (status.readEndpoints ?? []).map(entry => entry.nodeId).sort()
}

export async function collectNodeDiagnostics(environment: GateEnvironment): Promise<Record<string, unknown>> {
  const diagnostics: Record<string, unknown> = {
    group: await environment.etcdOracle.group().catch(err => String(err)),
    liveNodeIds: await environment.etcdOracle.liveNodeIds().catch(err => String(err)),
  }
  for (const [nodeId, node] of environment.nodes) {
    diagnostics[nodeId] = await statusOf(node).catch(err => ({
      error: err instanceof Error ? err.message : String(err),
      logs: node.logs(),
    }))
  }
  return diagnostics
}

export async function recentErrorCount(node: FailoverNodeProcess, code: string): Promise<number> {
  const status = await statusOf(node)
  return (status.recentErrors ?? []).filter(event => event.error?.code === code).length
}

export async function waitForRecentErrorCountAbove(
  node: FailoverNodeProcess,
  code: string,
  previousCount: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => recentErrorCount(node, code).then(count => count > previousCount), timeoutMs)
}

export async function waitForNoSafePrimaryReport(environment: GateEnvironment, timeoutMs = 15_000): Promise<void> {
  await waitForCondition(async () => {
    for (const node of environment.nodes.values()) {
      if ((await recentErrorCount(node, 'NO_SAFE_PRIMARY')) > 0) return true
    }
    return false
  }, timeoutMs)
}

export async function waitForNodeReady(node: FailoverNodeProcess, timeoutMs = 20_000): Promise<void> {
  let latestStatus: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latestStatus = await statusOf(node)
      return latestStatus.syncState?.phase === 'ready'
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `${node.config.nodeId} did not become ready: ${message}\nstatus=${describeValue(latestStatus)}\n${node.logs()}`,
    )
  }
}

export async function waitForStatus(
  node: FailoverNodeProcess,
  predicate: (status: FailoverStatus) => boolean,
  timeoutMs: number,
  description: string,
): Promise<FailoverStatus> {
  let latest: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latest = await statusOf(node)
      return predicate(latest)
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${node.config.nodeId} never showed ${description}: ${message}\nstatus=${describeValue(latest)}`)
  }
  if (latest === null) throw new Error(`${node.config.nodeId} status was never read`)
  return latest
}

export async function waitForPeerConnected(
  node: FailoverNodeProcess,
  peerId: string,
  timeoutMs: number,
): Promise<void> {
  let latestStatus: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latestStatus = await statusOf(node)
      return latestStatus.peers.some(peer => peer.nodeId === peerId && peer.connected)
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `${node.config.nodeId} did not connect to ${peerId}: ${message}\nstatus=${describeValue(latestStatus)}`,
    )
  }
}

export async function waitForReadiness(
  environment: GateEnvironment,
  nodeId: string,
  predicate: (readiness: ReadinessResponse) => boolean,
  timeoutMs: number,
  description: string,
): Promise<ReadinessResponse> {
  let latest: ReadinessResponse | null = null
  try {
    await waitForCondition(async () => {
      latest = await readinessOf(environment, nodeId)
      return predicate(latest)
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${nodeId} readiness never showed ${description}: ${message}\nlatest=${describeValue(latest)}`)
  }
  if (latest === null) throw new Error(`${nodeId} readiness was never read`)
  return latest
}

export async function waitForReadableNodes(
  environment: GateEnvironment,
  askedNodeId: string,
  expected: readonly string[],
  timeoutMs: number,
): Promise<ClusterStatusResponse> {
  let latest: ClusterStatusResponse | null = null
  const sortedExpected = [...expected].sort()
  try {
    await waitForCondition(async () => {
      latest = await clusterStatusOf(environment, askedNodeId)
      const listed = (latest.readEndpoints ?? []).map(entry => entry.nodeId).sort()
      return listed.join(',') === sortedExpected.join(',')
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `${askedNodeId} never listed exactly ${sortedExpected.join(',')} as readable: ${message}\nlatest=${describeValue(latest)}`,
    )
  }
  if (latest === null) throw new Error(`${askedNodeId} cluster status was never read`)
  return latest
}
