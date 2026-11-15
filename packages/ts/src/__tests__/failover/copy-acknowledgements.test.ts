import { describe, expect, it } from 'vitest'
import { expectRejectsWith, waitForIdenticalFiles } from './gate-assertions.js'
import { waitForInSyncSet } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  dbPathFor,
  type GateEnvironment,
  GROUP_ID,
  grpcProxyName,
  NODE_A,
  NODE_C,
  NODE_IDS,
  requireNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { statusOf, waitForStatus } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { holdsThroughout, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'
import { readHighestAppliedSeq, readHighestOwnChangeSeq, readItemIds, readMetaValue } from './sqlite-oracle.js'

const COPY_REQUEST_META_KEY = 'replication_copy_request_id'
const COPY_LATENCY_TOXIC = 'copy-delay'
const COPY_LATENCY_MS = 1_500
const COPY_LINKS = [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]

async function slowCopyLinks(env: GateEnvironment, latencyMs: number): Promise<void> {
  for (const link of COPY_LINKS) await env.toxiproxy.addLatency(link, COPY_LATENCY_TOXIC, latencyMs)
}

async function restoreCopyLinks(env: GateEnvironment): Promise<void> {
  for (const link of COPY_LINKS) await env.toxiproxy.clearToxic(link, COPY_LATENCY_TOXIC)
}

async function markRepairing(env: GateEnvironment, repairing: boolean): Promise<void> {
  await env.coordinator.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId: NODE_C, repairing })
}

async function waitForCopyStream(replica: FailoverNodeProcess): Promise<void> {
  await waitForStatus(
    replica,
    status => status.syncState?.phase === 'syncing' && (status.syncState.totalTables ?? 0) > 0,
    20_000,
    'the primary to stream the copy',
  )
}

async function currentTerm(env: GateEnvironment): Promise<bigint> {
  const group = await env.etcdOracle.group()
  if (!group) throw new Error('The coordinator holds no group state')
  return group.primaryTerm
}

const ROWS_BEFORE_THE_COPY = [1, 2, 3].map(id => itemRow(id, NODE_A, 'before-the-copy'))

async function writeBeforeTheCopy(env: GateEnvironment): Promise<void> {
  for (const row of ROWS_BEFORE_THE_COPY) await writeItemMajority(requireNode(env, NODE_A), row)
  await waitForInSyncSet(env, NODE_IDS, 15_000)
}

describe('acknowledgements from a replica that a full copy rebuilds', () => {
  const environment = useFailoverEnvironment()

  it('keeps the replica out of the in-sync set when an acknowledgement that it sent before the copy reaches the primary after the reset', async () => {
    const env = environment()
    await startCluster(env)
    await writeBeforeTheCopy(env)
    const replica = requireNode(env, NODE_C)
    const earlierCopy = readMetaValue(dbPathFor(env, NODE_C), COPY_REQUEST_META_KEY) ?? undefined
    const appliedBeforeTheCopy = readHighestAppliedSeq(dbPathFor(env, NODE_C), NODE_A)
    expect(appliedBeforeTheCopy).toBeGreaterThanOrEqual(readHighestOwnChangeSeq(dbPathFor(env, NODE_A), NODE_A))

    await slowCopyLinks(env, COPY_LATENCY_MS)
    await markRepairing(env, true)
    await waitForCopyStream(replica)
    await markRepairing(env, false)
    await replica.sendRawAck(NODE_A, {
      batchId: '',
      ackedSeq: appliedBeforeTheCopy,
      nodeId: NODE_C,
      groupId: GROUP_ID,
      primaryTerm: await currentTerm(env),
      copyRequestId: earlierCopy,
    })

    await holdsThroughout(
      async () => {
        const status = await statusOf(replica)
        const group = await env.etcdOracle.group()
        return status.syncState?.phase === 'ready' || group?.inSyncNodeIds.includes(NODE_C) === false
      },
      4_000,
      'the replica to stay outside the in-sync set until it acknowledges the copy',
    )
    await restoreCopyLinks(env)
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual(ROWS_BEFORE_THE_COPY)
  })

  it('counts an acknowledgement without a copy request ID toward no write concern after the primary resets the replica for a copy', async () => {
    const env = environment()
    await startCluster(env)
    await writeBeforeTheCopy(env)
    const replica = requireNode(env, NODE_C)

    await slowCopyLinks(env, COPY_LATENCY_MS)
    await markRepairing(env, true)
    await waitForCopyStream(replica)
    const primaryPath = dbPathFor(env, NODE_A)
    const before = readHighestOwnChangeSeq(primaryPath, NODE_A)
    const write = requireNode(env, NODE_A).execute(
      'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
      [4, NODE_A, 4, 'all-during-the-copy'],
      { writeConcern: { level: 'all', timeoutMs: 4_000 } },
    )
    const rejection = expectRejectsWith(write, ['WRITE_CONCERN_ERROR'])
    await waitForCondition(async () => readHighestOwnChangeSeq(primaryPath, NODE_A) > before, 5_000)
    await replica.sendRawAck(NODE_A, {
      batchId: '',
      ackedSeq: readHighestOwnChangeSeq(primaryPath, NODE_A),
      nodeId: NODE_C,
      groupId: GROUP_ID,
      primaryTerm: await currentTerm(env),
    })

    await rejection
    expect(readItemIds(dbPathFor(env, NODE_C))).not.toContain(4)
    await restoreCopyLinks(env)
    await markRepairing(env, false)
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual([
      ...ROWS_BEFORE_THE_COPY,
      itemRow(4, NODE_A, 'all-during-the-copy'),
    ])
  })
})
