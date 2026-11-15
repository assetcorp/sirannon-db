import { describe, expect, it } from 'vitest'
import { expectRejectsWith, waitForIdenticalFiles } from './gate-assertions.js'
import { waitForInSyncSet } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  dbPathFor,
  type GateEnvironment,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForStatus } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { restartNode, startCluster } from './gate-scenarios.js'
import { holdsThroughout, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'
import { readHighestAppliedSeq, readHighestOwnChangeSeq, readItemIds, readMetaValue } from './sqlite-oracle.js'

const COPY_REQUEST_META_KEY = 'replication_copy_request_id'

async function markRepairing(env: GateEnvironment, repairing: boolean): Promise<void> {
  await env.coordinator.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId: NODE_C, repairing })
}

async function waitForCopyStream(primary: FailoverNodeProcess): Promise<void> {
  await waitForCondition(async () => (await primary.copyTraffic('count')) > 0, 10_000)
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

  it('keeps the replica out of the in-sync set when an acknowledgement carries an earlier copy ID after the reset', async () => {
    const env = environment()
    await startCluster(env)
    await writeBeforeTheCopy(env)
    const replica = requireNode(env, NODE_C)
    const primary = requireNode(env, NODE_A)
    const earlierCopy = readMetaValue(dbPathFor(env, NODE_C), COPY_REQUEST_META_KEY) ?? undefined
    expect(earlierCopy).toBeTypeOf('string')
    expect(earlierCopy).not.toBe('')
    const appliedBeforeTheCopy = readHighestAppliedSeq(dbPathFor(env, NODE_C), NODE_A)
    expect(appliedBeforeTheCopy).toBeGreaterThanOrEqual(readHighestOwnChangeSeq(dbPathFor(env, NODE_A), NODE_A))

    await primary.copyTraffic('hold', 'batch', NODE_C)
    await markRepairing(env, true)
    await waitForCopyStream(primary)
    await markRepairing(env, false)
    await replica.sendRawAck(NODE_A, {
      batchId: '',
      ackedSeq: appliedBeforeTheCopy + 1n,
      nodeId: NODE_C,
      groupId: GROUP_ID,
      primaryTerm: await currentTerm(env),
      copyRequestId: earlierCopy,
    })
    await waitForStatus(
      primary,
      status =>
        status.peers.some(peer => peer.nodeId === NODE_C && BigInt(peer.lastAckedSeq) === appliedBeforeTheCopy + 1n),
      2_000,
      'the stale acknowledgement processed while the copy is held',
    )

    await holdsThroughout(
      async () => {
        const group = await env.etcdOracle.group()
        return group?.inSyncNodeIds.includes(NODE_C) === false
      },
      500,
      'the replica to stay outside the in-sync set until it acknowledges the copy',
    )
    await primary.copyTraffic('release')
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual(ROWS_BEFORE_THE_COPY)
  })

  it('counts no acknowledgement with a missing, empty, forged, or oversized copy request ID toward a write concern after the reset', async () => {
    const env = environment()
    await startCluster(env)
    await writeBeforeTheCopy(env)
    const replica = requireNode(env, NODE_C)
    const primary = requireNode(env, NODE_A)

    await primary.copyTraffic('hold', 'batch', NODE_C)
    await markRepairing(env, true)
    await waitForCopyStream(primary)
    const primaryPath = dbPathFor(env, NODE_A)
    const before = readHighestOwnChangeSeq(primaryPath, NODE_A)
    const write = requireNode(env, NODE_A).execute(
      'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
      [4, NODE_A, 4, 'all-during-the-copy'],
      { writeConcern: { level: 'all', timeoutMs: 4_000 } },
    )
    let writeSettled = false
    void write.then(
      () => {
        writeSettled = true
      },
      () => {
        writeSettled = true
      },
    )
    const rejection = expectRejectsWith(write, ['WRITE_CONCERN_ERROR'])
    await waitForCondition(async () => readHighestOwnChangeSeq(primaryPath, NODE_A) > before, 5_000)
    const copyRequestIds = [undefined, '', 'uncompleted-copy', 'x'.repeat(1024 * 1024)]
    const term = await currentTerm(env)
    const writtenSeq = readHighestOwnChangeSeq(primaryPath, NODE_A)
    await waitForStatus(
      primary,
      status => status.peers.some(peer => peer.nodeId === NODE_B && BigInt(peer.lastAckedSeq) >= writtenSeq),
      2_000,
      'the other voter to acknowledge the write before the forged acknowledgements',
    )
    for (const [index, copyRequestId] of copyRequestIds.entries()) {
      await replica.sendRawAck(NODE_A, {
        batchId: '',
        ackedSeq: writtenSeq + BigInt(index),
        nodeId: NODE_C,
        groupId: GROUP_ID,
        primaryTerm: term,
        copyRequestId,
      })
    }
    await waitForStatus(
      primary,
      status => status.peers.some(peer => peer.nodeId === NODE_C && BigInt(peer.lastAckedSeq) === writtenSeq + 3n),
      2_000,
      'every invalid copy proof processed before the copy can finish',
    )
    expect(writeSettled, 'the write must remain pending after the primary processes every invalid copy proof').toBe(
      false,
    )

    await rejection
    expect(readItemIds(dbPathFor(env, NODE_C))).not.toContain(4)
    await primary.copyTraffic('release')
    await markRepairing(env, false)
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual([
      ...ROWS_BEFORE_THE_COPY,
      itemRow(4, NODE_A, 'all-during-the-copy'),
    ])
  })

  it('keeps the completed copy ID on disk when the replica restarts before its first acknowledgement', async () => {
    const env = environment()
    await startCluster(env)
    await writeBeforeTheCopy(env)
    const replica = requireNode(env, NODE_C)
    const primary = requireNode(env, NODE_A)
    const replicaPath = dbPathFor(env, NODE_C)
    const earlierCopy = readMetaValue(replicaPath, COPY_REQUEST_META_KEY)
    await replica.copyTraffic('hold', 'ack', NODE_A)
    await markRepairing(env, true)
    await waitForCondition(async () => {
      const recorded = readMetaValue(replicaPath, COPY_REQUEST_META_KEY)
      return recorded !== null && recorded !== earlierCopy && (await replica.copyTraffic('count')) > 0
    }, 15_000)
    const completedCopy = readMetaValue(replicaPath, COPY_REQUEST_META_KEY)
    expect(readMetaValue(replicaPath, 'replication_anchored_term')).toBe(String(await currentTerm(env)))
    expect((await env.etcdOracle.group())?.inSyncNodeIds).not.toContain(NODE_C)

    await killNode(env, NODE_C)
    expect(readMetaValue(replicaPath, COPY_REQUEST_META_KEY)).toBe(completedCopy)
    await primary.copyTraffic('hold', 'batch', NODE_C)
    const restarting = restartNode(env, NODE_C)
    await waitForCopyStream(primary)
    expect(readMetaValue(replicaPath, COPY_REQUEST_META_KEY)).toBe(completedCopy)
    await primary.copyTraffic('release')
    await restarting
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual(ROWS_BEFORE_THE_COPY)
  })
})
