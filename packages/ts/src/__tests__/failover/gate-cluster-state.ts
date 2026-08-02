import { expect } from 'vitest'
import { CLUSTER_ID, type GateEnvironment, GROUP_ID, NODE_IDS, requireNode } from './gate-environment.js'
import {
  collectNodeDiagnostics,
  type FailoverStatus,
  readinessOf,
  recentErrorCount,
  statusOf,
} from './gate-observations.js'
import { jsonReplacer, sameMembers, waitForCondition } from './gate-support.js'

let writeProbeId = 1_000

function nextWriteProbeId(): number {
  writeProbeId += 1
  return writeProbeId
}

export async function expectExactlyOneWritablePrimary(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: string,
): Promise<void> {
  let latestStatuses: FailoverStatus[] = []
  try {
    await waitForCondition(async () => {
      latestStatuses = await Promise.all([...environment.nodes.values()].map(statusOf))
      const authoritative = latestStatuses.filter(status => status.coordinator?.authority)
      return (
        authoritative.length === 1 &&
        authoritative[0]?.nodeId === expectedPrimary &&
        authoritative[0]?.coordinator?.primaryTerm === expectedTerm
      )
    }, 20_000)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Did not observe exactly one authoritative primary ${expectedPrimary} term ${expectedTerm}: ${message}\nstatuses=${JSON.stringify(
        latestStatuses,
        jsonReplacer,
        2,
      )}`,
    )
  }

  const probeResults: Array<{ nodeId: string; ok: boolean; code?: string; message?: string }> = []
  const nodes = [...environment.nodes.entries()].sort(([left], [right]) => left.localeCompare(right))
  for (const [nodeId, node] of nodes) {
    try {
      await node.localWriteProbe(nextWriteProbeId(), `primary-probe-term-${expectedTerm}-${nodeId}`)
      probeResults.push({ nodeId, ok: true })
    } catch (err: unknown) {
      const error = err as Error & { code?: string }
      probeResults.push({ nodeId, ok: false, code: error.code, message: error.message })
    }
  }

  const writable = probeResults.filter(result => result.ok)
  expect(writable, `local write probe results=${JSON.stringify(probeResults, null, 2)}`).toEqual([
    { nodeId: expectedPrimary, ok: true },
  ])
}

export async function assertHealth(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: string,
  expectedReadAvailability: 'available' | 'unavailable',
  expectedWriteAvailability: 'available' | 'unavailable',
): Promise<void> {
  const primaryStatus = await statusOf(requireNode(environment, expectedPrimary))
  expect(primaryStatus.coordinator?.currentPrimary?.nodeId).toBe(expectedPrimary)
  expect(primaryStatus.coordinator?.primaryTerm).toBe(expectedTerm)
  const ready = await readinessOf(environment, expectedPrimary)
  expect(ready.replication?.currentPrimary).toBe(expectedPrimary)
  expect(ready.replication?.primaryTerm).toBe(expectedTerm)
  expect(ready.replication?.readAvailability).toBe(expectedReadAvailability)
  expect(ready.replication?.writeAvailability).toBe(expectedWriteAvailability)
  expect(ready.replication?.syncState).toBe('ready')
  expect(ready.replication?.coordinator?.authority).toBe(true)
  expect(primaryStatus.coordinator?.repairingNodeIds ?? []).toEqual(expect.any(Array))
  expect(primaryStatus.coordinator?.faultedNodeIds ?? []).toEqual(expect.any(Array))
}

export async function waitForInSyncSet(
  environment: GateEnvironment,
  expectedNodeIds: readonly string[],
  timeoutMs: number,
): Promise<void> {
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return sameMembers((latestState as { inSyncNodeIds?: string[] } | null)?.inSyncNodeIds ?? [], expectedNodeIds)
    }, timeoutMs)
  } catch (err: unknown) {
    const statuses: Record<string, unknown> = {}
    for (const [nodeId, node] of environment.nodes) {
      statuses[nodeId] = await statusOf(node).catch(statusErr => ({
        error: statusErr instanceof Error ? statusErr.message : String(statusErr),
        logs: node.logs(),
      }))
    }
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for in-sync set ${expectedNodeIds.join(',')}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nstatuses=${JSON.stringify(statuses, jsonReplacer, 2)}`,
    )
  }
}

export async function waitForCurrentPrimary(
  environment: GateEnvironment,
  expectedNodeId: string,
  timeoutMs: number,
): Promise<void> {
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return (latestState as { currentPrimary?: { nodeId?: string } } | null)?.currentPrimary?.nodeId === expectedNodeId
    }, timeoutMs)
  } catch (err: unknown) {
    const liveSessions: Record<string, unknown> = {}
    for (const nodeId of NODE_IDS) {
      liveSessions[nodeId] = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId).catch(sessionErr => ({
        error: sessionErr instanceof Error ? sessionErr.message : String(sessionErr),
      }))
    }
    const nodeStatuses: Record<string, unknown> = {}
    for (const [nodeId, node] of environment.nodes) {
      nodeStatuses[nodeId] = await statusOf(node).catch(statusErr => ({
        error: statusErr instanceof Error ? statusErr.message : String(statusErr),
        logs: node.logs(),
      }))
    }
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for primary ${expectedNodeId}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nliveSessions=${JSON.stringify(liveSessions, jsonReplacer, 2)}\nnodeStatuses=${JSON.stringify(
        nodeStatuses,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

export async function waitForAnyCurrentPrimary(
  environment: GateEnvironment,
  expectedNodeIds: readonly string[],
  timeoutMs: number,
): Promise<string> {
  let currentPrimary: string | null = null
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      currentPrimary = (latestState as { currentPrimary?: { nodeId?: string } } | null)?.currentPrimary?.nodeId ?? null
      return currentPrimary !== null && expectedNodeIds.includes(currentPrimary)
    }, timeoutMs)
  } catch (err: unknown) {
    const liveSessions: Record<string, unknown> = {}
    for (const nodeId of NODE_IDS) {
      liveSessions[nodeId] = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId).catch(sessionErr => ({
        error: sessionErr instanceof Error ? sessionErr.message : String(sessionErr),
      }))
    }
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for any primary in ${expectedNodeIds.join(',')}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nliveSessions=${JSON.stringify(liveSessions, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
  if (currentPrimary === null) {
    throw new Error(`No current primary matched ${expectedNodeIds.join(',')}`)
  }
  return currentPrimary
}

export async function waitForNoSafePrimary(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const node = requireNode(environment, nodeId)
      if ((await recentErrorCount(node, 'NO_SAFE_PRIMARY')) > 0) {
        return true
      }
    }
    return false
  }, timeoutMs)
}
