import { describe, expect, it } from 'vitest'
import { waitForItemsOnDisk } from './gate-assertions.js'
import { waitForGroup, waitForInSyncSet } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
  httpEndpointFor,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  setEtcdLink,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { clusterStatusOf, type ReadinessResponse, readinessOf, waitForReadiness } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { waitForCondition } from './gate-support.js'

const CONTROLLERS_OFF = { everyNode: { controllerEnabled: false } }

async function waitForHealth(
  env: GateEnvironment,
  nodeId: string,
  status: ReadinessResponse['status'],
  reason: string,
): Promise<ReadinessResponse> {
  return waitForReadiness(
    env,
    nodeId,
    readiness => readiness.status === status && readiness.replication?.healthReason === reason,
    15_000,
    `${status} because ${reason}`,
  )
}

async function setMaintenance(
  env: GateEnvironment,
  nodeId: string,
  flags: { draining?: boolean; repairing?: boolean },
): Promise<void> {
  await env.coordinator.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId, ...flags })
}

describe('the health each node reports', () => {
  const environment = useFailoverEnvironment()

  it('reports a replica as lagging, cut off from etcd, draining, or repairing, with repair named ahead of every other reason', async () => {
    const env = environment()
    await startCluster(env, CONTROLLERS_OFF)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'everyone-healthy'))
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    for (const nodeId of NODE_IDS) {
      const readiness = await waitForHealth(env, nodeId, 'ok', 'in-sync')
      expect(readiness.replication?.readAvailability).toBe('available')
      expect(readiness.replication?.coordinator?.connected).toBe(true)
      expect(readiness.replication?.controller?.state).toBe('disabled')
    }

    await setReplicationLinks(env, NODE_C, false)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(2, NODE_A, 'c-falls-behind'))
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    const lagging = await waitForHealth(env, NODE_C, 'degraded', 'lagging')
    expect(lagging.replication?.readAvailability).toBe('available')
    expect(lagging.replication?.writeAvailability).toBe('unavailable')
    const fromPrimary = await readinessOf(env, NODE_A)
    expect(fromPrimary.replication?.laggingReplicas).toEqual([NODE_C])
    expect(fromPrimary.replication?.inSyncReplicas).toEqual([NODE_B])

    await setEtcdLink(env, NODE_C, false)
    const unreachable = await waitForHealth(env, NODE_C, 'degraded', 'coordinator-unreachable')
    expect(unreachable.replication?.coordinator?.connected).toBe(false)
    await setEtcdLink(env, NODE_C, true)
    await setReplicationLinks(env, NODE_C, true)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(3, NODE_A, 'c-catches-up'))
    await waitForInSyncSet(env, NODE_IDS, 20_000)
    await waitForHealth(env, NODE_C, 'ok', 'in-sync')

    await setMaintenance(env, NODE_B, { draining: true })
    const draining = await waitForHealth(env, NODE_B, 'unavailable', 'draining')
    expect(draining.replication?.readAvailability).toBe('unavailable')

    await setReplicationLinks(env, NODE_B, false)
    await setMaintenance(env, NODE_B, { repairing: true })
    const repairing = await waitForHealth(env, NODE_B, 'repairing', 'repairing')
    expect(repairing.replication?.readAvailability).toBe('unavailable')
    expect(repairing.replication?.writeAvailability).toBe('unavailable')
  })

  it('reports the role that etcd records over the configured one, and a draining primary that keeps authority as failing over', async () => {
    const env = environment()
    await startCluster(env, CONTROLLERS_OFF)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'before-the-handover'))
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    const healthy = await waitForHealth(env, NODE_A, 'ok', 'in-sync')
    expect(healthy.replication?.role).toBe('primary')
    expect(healthy.replication?.writeAvailability).toBe('available')
    expect(healthy.replication?.coordinator?.authority).toBe(true)

    await setMaintenance(env, NODE_A, { draining: true })
    const failingOver = await waitForHealth(env, NODE_A, 'failing_over', 'draining')
    expect(failingOver.replication?.coordinator?.authority).toBe(true)
    expect(failingOver.replication?.writeAvailability).toBe('unavailable')
    expect(failingOver.replication?.readAvailability).toBe('unavailable')
    expect((await clusterStatusOf(env, NODE_A)).health).toBe('failing_over')

    await setMaintenance(env, NODE_A, { draining: false })
    const handover = await env.coordinator.compareAndAdvancePrimaryTerm({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      expectedPrimaryTerm: 1n,
      nextPrimary: { nodeId: NODE_B, endpoint: httpEndpointFor(env, NODE_B) },
    })
    expect(handover.advanced).toBe(true)
    const promoted = await waitForReadiness(
      env,
      NODE_B,
      readiness => readiness.replication?.role === 'primary' && readiness.replication.primaryTerm === '2',
      10_000,
      'the role etcd records',
    )
    expect(promoted.replication?.currentPrimary).toBe(NODE_B)
    const demoted = await waitForReadiness(
      env,
      NODE_A,
      readiness => readiness.replication?.role === 'replica' && readiness.replication.currentPrimary === NODE_B,
      10_000,
      'the demotion etcd records',
    )
    expect(demoted.replication?.coordinator?.authority).toBe(false)
    await waitForCondition(async () => (await clusterStatusOf(env, NODE_C)).currentPrimary?.nodeId === NODE_B, 10_000)

    const afterHandover = itemRow(2, NODE_B, 'after-the-handover')
    await writeItemMajority(requireNode(env, NODE_B), afterHandover)
    await waitForGroup(env, group => group.inSyncNodeIds.includes(NODE_A), 30_000, `${NODE_A} rejoined as a replica`)
    await waitForItemsOnDisk(env, NODE_IDS, [itemRow(1, NODE_A, 'before-the-handover'), afterHandover], 20_000)
    await waitForHealth(env, NODE_A, 'ok', 'in-sync')
  })
})
