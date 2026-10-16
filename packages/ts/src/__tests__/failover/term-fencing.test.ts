import { describe, expect, it } from 'vitest'
import {
  expectItemsAbsentOnDisk,
  expectRejectsWith,
  waitForEventOnDisk,
  waitForItemsOnDisk,
} from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  waitForGroup,
  waitForInSyncSet,
  waitForPrimary,
} from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  GROUP_ID,
  httpEndpointFor,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForPeerConnected } from './gate-observations.js'
import {
  executePublicClient,
  executePublicHttp,
  executePublicWebSocket,
  itemRow,
  postSql,
  writeItemMajority,
} from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import {
  expectStaleBatchRefused,
  expectStaleForwardRefused,
  expectStaleSyncBatchRefused,
  expectStaleSyncRequestRefused,
} from './gate-stale-messages.js'
import { holdsThroughout, waitForCondition } from './gate-support.js'

const EVENT_SQL = (id: number, kind: string) =>
  `INSERT INTO failover_events (id, item_id, kind, detail) VALUES (${id}, 1, '${kind}', 'after-failover')`

describe('the primary term on every message', () => {
  const environment = useFailoverEnvironment()

  it('refuses batches, forwards, and sync traffic stamped with the term before a failover', async () => {
    const env = environment()
    await startCluster(env)
    const shared = itemRow(1, NODE_A, 'before-failover')
    await writeItemMajority(requireNode(env, NODE_A), shared)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    const promoted = promotion.currentPrimary.nodeId
    const replica = promoted === NODE_B ? NODE_C : NODE_B
    await requireNode(env, replica).reconnectTransport()
    await waitForPeerConnected(requireNode(env, replica), promoted, 15_000)

    const toPrimary = { senderNodeId: replica, receiverNodeId: promoted, staleTerm: 1n }
    const toReplica = { senderNodeId: promoted, receiverNodeId: replica, staleTerm: 1n }
    await expectStaleBatchRefused(env, toPrimary, 70)
    await expectStaleBatchRefused(env, toReplica, 71)
    await expectStaleForwardRefused(env, toPrimary, 72)
    await expectStaleSyncRequestRefused(env, toPrimary, 73)
    await expectStaleSyncBatchRefused(env, toReplica, 74)
    await expectStaleForwardRefused(env, { ...toPrimary, staleTerm: 3n }, 75)

    await waitForItemsOnDisk(env, [promoted, replica], [shared], 10_000)
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })

  it('fails with STALE_PRIMARY a majority write whose in-sync update reaches etcd after its primary is demoted and rejoins, and leaves the new term untouched', async () => {
    const env = environment()
    await startCluster(env)
    const shared = itemRow(1, NODE_A, 'before-the-cut')
    await writeItemMajority(requireNode(env, NODE_A), shared)
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await setReplicationLinks(env, NODE_A, false, [NODE_B])
    const demoted = requireNode(env, NODE_A)
    await demoted.holdInSyncUpdates({ removesNodeId: NODE_B })
    const inFlight = itemRow(2, NODE_A, 'acknowledged-by-c-during-the-demotion')
    const write = expectRejectsWith(writeItemMajority(demoted, inFlight, 15_000, 120_000), ['STALE_PRIMARY'])
    await waitForCondition(async () => (await demoted.heldInSyncUpdates()) > 0, 15_000)
    await setReplicationLinks(env, NODE_A, true, [NODE_B])
    await waitForItemsOnDisk(env, NODE_IDS, [shared, inFlight], 15_000)

    const advanced = await env.coordinator.compareAndAdvancePrimaryTerm({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      expectedPrimaryTerm: 1n,
      nextPrimary: { nodeId: NODE_B, endpoint: httpEndpointFor(env, NODE_B) },
    })
    expect(advanced.advanced).toBe(true)
    await waitForGroup(
      env,
      group =>
        group.primaryTerm === 2n &&
        NODE_IDS.every(nodeId => group.inSyncNodeIds.includes(nodeId)) &&
        !group.repairingNodeIds.includes(NODE_A),
      60_000,
      `${NODE_A} rebuilt and back in the in-sync set of term 2`,
    )
    await demoted.releaseInSyncUpdates()
    await write

    await holdsThroughout(
      async () => {
        const group = await env.etcdOracle.group()
        return (
          group?.primaryTerm === 2n && group.currentPrimary?.nodeId === NODE_B && group.inSyncNodeIds.includes(NODE_B)
        )
      },
      3_000,
      `etcd to keep ${NODE_B} as primary and in the in-sync set at term 2`,
    )
    await expectExactlyOneWritablePrimary(env, NODE_B, 2n)
  })

  it('forwards writes sent to a replica to the promoted primary over the engine, HTTP, the client, and WebSocket, and refuses a callback transaction on the replica', async () => {
    const env = environment()
    await startCluster(env)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'parent-row'))
    await executePublicHttp(env, NODE_A, "UPDATE failover_items SET note = 'updated-over-http' WHERE id = 1")
    await waitForItemsOnDisk(env, [NODE_A, NODE_B, NODE_C], [itemRow(1, NODE_A, 'updated-over-http')], 15_000)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    const promoted = promotion.currentPrimary.nodeId
    const replica = promoted === NODE_B ? NODE_C : NODE_B
    await requireNode(env, replica).reconnectTransport()
    await waitForPeerConnected(requireNode(env, replica), promoted, 15_000)

    await requireNode(env, replica).execute(EVENT_SQL(10, 'engine'))
    await waitForEventOnDisk(env, [promoted, replica], 10, 20_000)
    await executePublicHttp(env, replica, EVENT_SQL(11, 'http'))
    await waitForEventOnDisk(env, [promoted, replica], 11, 20_000)
    await executePublicClient(env, replica, EVENT_SQL(12, 'client'))
    await waitForEventOnDisk(env, [promoted, replica], 12, 20_000)
    await executePublicWebSocket(env, replica, EVENT_SQL(13, 'websocket'))
    await waitForEventOnDisk(env, [promoted, replica], 13, 20_000)

    const statementList = await postSql(env, replica, 'transaction', {
      statements: [{ sql: EVENT_SQL(14, 'statement-list') }, { sql: EVENT_SQL(15, 'statement-list') }],
    })
    expect(statementList.status, JSON.stringify(statementList.body)).toBe(200)
    await waitForEventOnDisk(env, [promoted, replica], 14, 20_000)
    await waitForEventOnDisk(env, [promoted, replica], 15, 20_000)

    await expectRejectsWith(requireNode(env, replica).localWriteProbe(16, 'callback-transaction-on-a-replica'), [
      'TOPOLOGY_ERROR',
    ])
    await expectItemsAbsentOnDisk(env, [promoted, replica], [16], 1_000)
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })
})
