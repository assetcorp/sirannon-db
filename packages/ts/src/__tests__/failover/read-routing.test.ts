import { describe, expect, it } from 'vitest'
import { expectItemsAbsentOnDisk, waitForItemsOnDisk } from './gate-assertions.js'
import { waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  type GateEnvironment,
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
import { clusterStatusOf, waitForReadableNodes } from './gate-observations.js'
import { executePublicClient, itemRow, postSql, writeItemMajority } from './gate-public-api.js'
import { restartNode, startCluster } from './gate-scenarios.js'
import { waitForCondition } from './gate-support.js'

const SELECT_IDS = 'SELECT id FROM failover_items ORDER BY id'

function rowIds(body: Record<string, unknown>): number[] {
  const rows = body.rows
  if (!Array.isArray(rows)) throw new Error(`The query answer holds no rows: ${JSON.stringify(body)}`)
  return rows.map(row => Number((row as { id: unknown }).id))
}

async function expectRefusedWithPrimary(
  env: GateEnvironment,
  askedNodeId: string,
  rowId: number,
  primaryNodeId: string,
  primaryTerm: string,
): Promise<void> {
  const answer = await postSql(env, askedNodeId, 'execute', {
    sql: `INSERT INTO failover_items (id, owner, value, note) VALUES (${rowId}, 'http', ${rowId}, 'sent-to-a-replica')`,
  })
  expect(answer.status).toBe(409)
  expect(answer.body.error?.code).toBe('STALE_PRIMARY')
  expect(answer.body.error?.details?.currentPrimary).toEqual({
    nodeId: primaryNodeId,
    endpoint: httpEndpointFor(env, primaryNodeId),
  })
  expect(answer.body.error?.details?.primaryTerm).toBe(primaryTerm)
}

describe('where clients can read and write', () => {
  const environment = useFailoverEnvironment()

  it('drops a killed node from every read list, lists it again once it returns, and serves majority reads only from in-sync nodes', async () => {
    const env = environment()
    await startCluster(env)
    const first = itemRow(1, NODE_A, 'read-by-everyone')
    await writeItemMajority(requireNode(env, NODE_A), first)
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    for (const nodeId of NODE_IDS) {
      await waitForReadableNodes(env, nodeId, NODE_IDS, 10_000)
      await waitForCondition(async () => {
        const entries = (await clusterStatusOf(env, nodeId)).readEndpoints ?? []
        return entries.every(entry => entry.readConcerns.join(',') === 'local,majority')
      }, 10_000)
      for (const entry of (await clusterStatusOf(env, nodeId)).readEndpoints ?? []) {
        expect(entry.endpoint).toBe(httpEndpointFor(env, entry.nodeId))
      }
    }

    await killNode(env, NODE_C)
    await waitForReadableNodes(env, NODE_A, [NODE_A, NODE_B], 10_000)
    await waitForReadableNodes(env, NODE_B, [NODE_A, NODE_B], 10_000)
    await restartNode(env, NODE_C)
    await waitForReadableNodes(env, NODE_A, NODE_IDS, 15_000)
    await waitForInSyncSet(env, NODE_IDS, 20_000)

    await setReplicationLinks(env, NODE_C, false)
    const second = itemRow(2, NODE_A, 'c-never-sees-it')
    await writeItemMajority(requireNode(env, NODE_A), second)
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    const fromPrimary = await waitForReadableNodes(env, NODE_A, NODE_IDS, 10_000)
    expect(fromPrimary.readEndpoints?.find(entry => entry.nodeId === NODE_C)?.readConcerns).toEqual(['local'])

    const majorityOnLagging = await postSql(env, NODE_C, 'query', {
      sql: SELECT_IDS,
      readConcern: { level: 'majority' },
    })
    expect(majorityOnLagging.status).toBe(503)
    expect(majorityOnLagging.body.error?.code).toBe('NODE_NOT_IN_SYNC')
    const localOnLagging = await postSql(env, NODE_C, 'query', { sql: SELECT_IDS, readConcern: { level: 'local' } })
    expect(localOnLagging.status).toBe(200)
    expect(rowIds(localOnLagging.body)).toEqual([1])
    const majorityInSync = await postSql(env, NODE_B, 'query', { sql: SELECT_IDS, readConcern: { level: 'majority' } })
    expect(majorityInSync.status).toBe(200)
    expect(rowIds(majorityInSync.body)).toEqual([1, 2])
  })

  it('refuses a write sent to a replica with the address of the primary when forwarding is off, and a topology-aware client follows it across a failover', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { writeForwarding: false } })
    await expectRefusedWithPrimary(env, NODE_B, 1, NODE_A, '1')
    await expectRefusedWithPrimary(env, NODE_C, 2, NODE_A, '1')
    await expectItemsAbsentOnDisk(env, NODE_IDS, [1, 2], 1_000)

    await executePublicClient(
      env,
      NODE_B,
      "INSERT INTO failover_items (id, owner, value, note) VALUES (3, 'client', 3, 'routed-to-the-primary')",
    )
    const routed = itemRow(3, 'client', 'routed-to-the-primary')
    await waitForItemsOnDisk(env, NODE_IDS, [routed], 15_000)
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    const promoted = promotion.currentPrimary.nodeId
    const survivor = promoted === NODE_B ? NODE_C : NODE_B
    await expect.poll(async () => (await clusterStatusOf(env, survivor)).currentPrimary?.nodeId).toBe(promoted)
    await expectRefusedWithPrimary(env, survivor, 4, promoted, '2')

    await executePublicClient(
      env,
      survivor,
      "INSERT INTO failover_items (id, owner, value, note) VALUES (5, 'client', 5, 'routed-after-failover')",
    )
    await waitForItemsOnDisk(env, [promoted, survivor], [routed, itemRow(5, 'client', 'routed-after-failover')], 15_000)
    await expectItemsAbsentOnDisk(env, [promoted, survivor], [1, 2, 4], 1_000)
  })
})
