import { describe, expect, it } from 'vitest'
import type { StoredNodeSession } from './etcd-oracle.js'
import { expectItemsAbsentOnDisk, expectRejectsWith, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  expectGroupHolds,
  waitForGroup,
  waitForSessionGone,
} from './gate-cluster-state.js'
import { CLUSTER_ID, GROUP_ID, NODE_A, NODE_B, NODE_C, NODE_IDS, requireNode, setEtcdLink } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForNoSafePrimaryReport, waitForStatus } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { sleep, waitForCondition } from './gate-support.js'

describe('the session a node holds in etcd', () => {
  const environment = useFailoverEnvironment()

  it('registers a primary whose session lapsed under a new lease and gives it back write authority at the same term', async () => {
    const env = environment()
    await startCluster(env)
    const first = itemRow(1, NODE_A, 'before-the-lapse')
    await writeItemMajority(requireNode(env, NODE_A), first)
    for (const nodeId of [NODE_B, NODE_C]) {
      await env.coordinator.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId, draining: true })
    }
    await waitForGroup(
      env,
      group => group.drainingNodeIds.includes(NODE_B) && group.drainingNodeIds.includes(NODE_C),
      5_000,
      'both replicas draining',
    )
    const before = await env.etcdOracle.nodeSession(NODE_A)
    if (!before) throw new Error(`${NODE_A} holds no session`)

    await setEtcdLink(env, NODE_A, false)
    await waitForSessionGone(env, NODE_A, 15_000)
    await expectRejectsWith(requireNode(env, NODE_A).localWriteProbe(50, 'while-the-session-is-gone'), [
      'COORDINATOR_UNAVAILABLE',
      'STALE_PRIMARY',
    ])
    await waitForNoSafePrimaryReport(env)
    await expectGroupHolds(env, NODE_A, 1n, 2_000)

    await setEtcdLink(env, NODE_A, true)
    const seen: { session?: StoredNodeSession | null } = {}
    await waitForCondition(async () => {
      seen.session = await env.etcdOracle.nodeSession(NODE_A)
      return seen.session !== null && seen.session.leaseId !== before.leaseId
    }, 15_000)
    expect(seen.session?.createRevision).toBeGreaterThan(before.createRevision)
    expect(seen.session?.etcdLeaseId).not.toBe(before.etcdLeaseId)
    await waitForStatus(requireNode(env, NODE_A), status => status.coordinator?.authority === true, 15_000, 'authority')

    const second = itemRow(2, NODE_A, 'after-the-new-lease')
    await writeItemMajority(requireNode(env, NODE_A), second)
    await waitForItemsOnDisk(env, NODE_IDS, [first, second], 15_000)
    await expectItemsAbsentOnDisk(env, NODE_IDS, [50], 1_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, 1n)
  })

  it('keeps the same session key, lease, and term through an etcd outage shorter than the lease', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { sessionTtlMs: 10_000, controllerLeaseTtlMs: 10_000 } })
    const first = itemRow(1, NODE_A, 'before-the-flap')
    await writeItemMajority(requireNode(env, NODE_A), first)
    const before = await env.etcdOracle.nodeSession(NODE_A)
    if (!before) throw new Error(`${NODE_A} holds no session`)

    await setEtcdLink(env, NODE_A, false)
    await sleep(2_500)
    await setEtcdLink(env, NODE_A, true)
    await waitForStatus(requireNode(env, NODE_A), status => status.coordinator?.authority === true, 15_000, 'authority')
    await sleep(2_500)

    const after = await env.etcdOracle.nodeSession(NODE_A)
    expect(after?.createRevision).toBe(before.createRevision)
    expect(after?.leaseId).toBe(before.leaseId)
    expect(after?.etcdLeaseId).toBe(before.etcdLeaseId)
    const second = itemRow(2, NODE_A, 'after-the-flap')
    await writeItemMajority(requireNode(env, NODE_A), second)
    await waitForItemsOnDisk(env, NODE_IDS, [first, second], 15_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, 1n)
  })
})
