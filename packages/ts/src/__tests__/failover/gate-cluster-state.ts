import { expect } from 'vitest'
import type { StoredGroup } from './etcd-oracle.js'
import { dbPathFor, type GateEnvironment } from './gate-environment.js'
import { collectNodeDiagnostics, statusOf } from './gate-observations.js'
import { describeValue, holdsThroughout, sameMembers, waitForCondition } from './gate-support.js'
import { readItemIds } from './sqlite-oracle.js'

let writeProbeId = 100_000

export async function waitForGroup(
  environment: GateEnvironment,
  predicate: (group: StoredGroup) => boolean,
  timeoutMs: number,
  description: string,
): Promise<StoredGroup> {
  let latest: StoredGroup | null = null
  try {
    await waitForCondition(async () => {
      latest = await environment.etcdOracle.group()
      return latest !== null && predicate(latest)
    }, timeoutMs)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `etcd never showed ${description}: ${message}\ngroup=${describeValue(latest)}\ndiagnostics=${describeValue(
        await collectNodeDiagnostics(environment),
      )}`,
    )
  }
  if (latest === null) throw new Error('etcd holds no group')
  return latest
}

export async function waitForInSyncSet(
  environment: GateEnvironment,
  expectedNodeIds: readonly string[],
  timeoutMs: number,
): Promise<StoredGroup> {
  return waitForGroup(
    environment,
    group => sameMembers(group.inSyncNodeIds, expectedNodeIds),
    timeoutMs,
    `the in-sync set ${expectedNodeIds.join(',')}`,
  )
}

export async function waitForPrimary(
  environment: GateEnvironment,
  candidates: readonly string[],
  timeoutMs: number,
): Promise<StoredGroup & { currentPrimary: { nodeId: string } }> {
  const group = await waitForGroup(
    environment,
    stored => stored.currentPrimary !== null && candidates.includes(stored.currentPrimary.nodeId),
    timeoutMs,
    `a primary among ${candidates.join(',')}`,
  )
  const currentPrimary = group.currentPrimary
  if (!currentPrimary) throw new Error('etcd names no primary')
  return { ...group, currentPrimary }
}

export async function expectGroupHolds(
  environment: GateEnvironment,
  primaryNodeId: string | null,
  primaryTerm: bigint,
  durationMs: number,
): Promise<void> {
  await holdsThroughout(
    async () => {
      const group = await environment.etcdOracle.group()
      return (group?.currentPrimary?.nodeId ?? null) === primaryNodeId && group?.primaryTerm === primaryTerm
    },
    durationMs,
    `etcd to keep ${primaryNodeId ?? 'no primary'} at term ${primaryTerm}`,
  )
}

export async function waitForControllerHolder(
  environment: GateEnvironment,
  candidates: readonly string[],
  timeoutMs: number,
): Promise<string> {
  const seen: { holder?: string } = {}
  await waitForCondition(async () => {
    seen.holder = (await environment.etcdOracle.controller())?.holderId
    return seen.holder !== undefined && candidates.includes(seen.holder)
  }, timeoutMs)
  if (seen.holder === undefined) throw new Error(`No controller holder among ${candidates.join(',')}`)
  return seen.holder
}

export async function waitForSessionGone(
  environment: GateEnvironment,
  nodeId: string,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => (await environment.etcdOracle.nodeSession(nodeId)) === null, timeoutMs)
}

export async function expectExactlyOneWritablePrimary(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: bigint,
): Promise<void> {
  const group = await waitForGroup(
    environment,
    stored => stored.currentPrimary?.nodeId === expectedPrimary && stored.primaryTerm === expectedTerm,
    20_000,
    `${expectedPrimary} as primary at term ${expectedTerm}`,
  )
  expect(group.currentPrimary?.nodeId).toBe(expectedPrimary)

  let latestStatuses: unknown[] = []
  try {
    await waitForCondition(async () => {
      const statuses = await Promise.all([...environment.nodes.values()].map(statusOf))
      latestStatuses = statuses
      const authoritative = statuses.filter(status => status.coordinator?.authority)
      return authoritative.length === 1 && authoritative[0]?.nodeId === expectedPrimary
    }, 20_000)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `More or fewer than one node claimed authority: ${message}\nstatuses=${describeValue(latestStatuses)}`,
    )
  }

  const results: Array<{ nodeId: string; accepted: boolean; code?: string }> = []
  const nodes = [...environment.nodes.entries()].sort(([left], [right]) => left.localeCompare(right))
  let acceptedProbeId: number | null = null
  for (const [nodeId, node] of nodes) {
    writeProbeId += 1
    try {
      await node.localWriteProbe(writeProbeId, `probe-term-${expectedTerm}`)
      results.push({ nodeId, accepted: true })
      if (nodeId === expectedPrimary) acceptedProbeId = writeProbeId
    } catch (err: unknown) {
      results.push({ nodeId, accepted: false, code: (err as { code?: string }).code })
    }
  }
  expect(
    results.filter(result => result.accepted),
    describeValue(results),
  ).toEqual([{ nodeId: expectedPrimary, accepted: true }])
  if (acceptedProbeId === null) throw new Error(`${expectedPrimary} accepted no probe`)
  expect(readItemIds(dbPathFor(environment, expectedPrimary))).toContain(acceptedProbeId)
}
