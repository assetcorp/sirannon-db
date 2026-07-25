import { type GateEnvironment, httpPortFor } from './gate-environment.js'
import { waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess, SerializedError } from './node-process.js'

export interface FailoverStatus {
  nodeId: string
  role: string
  peers: Array<{ nodeId: string; lastAckedSeq: string; connected: boolean }>
  localSeq: string
  replicating: boolean
  syncState?: {
    phase: string
    sourcePeerId: string | null
  }
  coordinator?: {
    currentPrimary: { nodeId: string; endpoint?: string } | null
    primaryTerm: string
    inSyncNodeIds: string[]
    drainingNodeIds: string[]
    repairingNodeIds: string[]
    faultedNodeIds: string[]
    votingDataBearingNodeIds: string[]
    authority: boolean
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
    primaryTerm?: string
    currentPrimary?: string
    readAvailability?: 'available' | 'unavailable'
    writeAvailability?: 'available' | 'unavailable'
    syncState?: string
    coordinator?: {
      authority: boolean
    }
    controller?: {
      state: string
    }
  }
}

export async function statusOf(node: FailoverNodeProcess): Promise<FailoverStatus> {
  return (await node.status()) as unknown as FailoverStatus
}

export async function readinessOf(environment: GateEnvironment, nodeId: string): Promise<ReadinessResponse> {
  const response = await fetch(`http://127.0.0.1:${httpPortFor(environment, nodeId)}/health/ready`)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Readiness for ${nodeId} returned ${response.status}: ${text}`)
  }
  return JSON.parse(text) as ReadinessResponse
}

export async function queryRows(
  node: FailoverNodeProcess,
  sql: string,
  params?: unknown[],
): Promise<Array<Record<string, unknown>>> {
  const value = await node.query(sql, params, { readConcern: { level: 'local' } })
  if (!Array.isArray(value)) {
    throw new Error('Query did not return an array')
  }
  return value as Array<Record<string, unknown>>
}

export async function queryRowsWhenReady(
  node: FailoverNodeProcess,
  sql: string,
  params?: unknown[],
): Promise<Array<Record<string, unknown>> | null> {
  const status = await statusOf(node)
  if (status.syncState?.phase !== 'ready') {
    return null
  }
  try {
    return await queryRows(node, sql, params)
  } catch (err: unknown) {
    if (isSyncPhaseReadError(err)) return null
    throw err
  }
}

function isSyncPhaseReadError(err: unknown): boolean {
  const error = err as Error & { code?: string }
  return error.code === 'SYNC_ERROR' && error.message.includes('cannot serve reads')
}

export async function collectNodeDiagnostics(environment: GateEnvironment): Promise<Record<string, unknown>> {
  const diagnostics: Record<string, unknown> = {}
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

export async function waitForNodeReady(node: FailoverNodeProcess, timeoutMs = 20_000): Promise<void> {
  let latestStatus: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latestStatus = await statusOf(node)
      return latestStatus.syncState?.phase === 'ready'
    }, timeoutMs)
  } catch (err: unknown) {
    const statusText = JSON.stringify(latestStatus, null, 2)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${node.config.nodeId} did not become ready: ${message}\nstatus=${statusText}\n${node.logs()}`)
  }
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
    const statusText = JSON.stringify(latestStatus, null, 2)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${node.config.nodeId} did not connect to ${peerId}: ${message}\nstatus=${statusText}`)
  }
}
