import { rmSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { waitForItemsOnDisk } from './gate-assertions.js'
import { waitForGroup, waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  dbPathFor,
  type GateEnvironment,
  GROUP_ID,
  grpcProxyName,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  setReplicationLinks,
  startNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { statusOf, waitForNodeReady } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { sleep, waitForCondition } from './gate-support.js'
import { type ItemRow, readItems } from './sqlite-oracle.js'

function wipeDatabaseFiles(env: GateEnvironment, nodeId: string): void {
  const dbPath = dbPathFor(env, nodeId)
  for (const suffix of ['', '-wal', '-shm', '-journal']) {
    rmSync(`${dbPath}${suffix}`, { force: true })
  }
}

async function setDraining(env: GateEnvironment, nodeId: string, draining: boolean): Promise<void> {
  await env.coordinator.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId, draining })
}

async function startEmptyNode(env: GateEnvironment, nodeId: string): Promise<void> {
  await waitForCondition(async () => (await env.etcdOracle.nodeSession(nodeId)) === null, 15_000)
  wipeDatabaseFiles(env, nodeId)
  await startNode(env, nodeId, { role: 'replica' })
}

async function writeRows(env: GateEnvironment, nodeId: string, ids: readonly number[]): Promise<ItemRow[]> {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, nodeId, 'before-the-move')
    await writeItemMajority(requireNode(env, nodeId), row)
    rows.push(row)
  }
  return rows
}

interface WriteLoad {
  acknowledged: ItemRow[]
  failures: number
  stop: () => Promise<void>
}

function startWriteLoad(env: GateEnvironment, firstId: number): WriteLoad {
  const load: WriteLoad = { acknowledged: [], failures: 0, stop: async () => undefined }
  let writing = true
  const loop = (async () => {
    for (let id = firstId; writing; id++) {
      const group = await env.etcdOracle.group().catch(() => null)
      const primaryId = group?.currentPrimary?.nodeId
      const primary = primaryId ? env.nodes.get(primaryId) : undefined
      if (!primary?.running) {
        await sleep(100)
        continue
      }
      const row = itemRow(id, primaryId ?? '', 'during-the-move')
      try {
        await writeItemMajority(primary, row, 3_000, 5_000)
        load.acknowledged.push(row)
      } catch {
        load.failures += 1
        await sleep(100)
      }
    }
  })()
  load.stop = async () => {
    writing = false
    await loop
  }
  return load
}

async function expectAcknowledgedRowsEverywhere(env: GateEnvironment, rows: readonly ItemRow[]): Promise<void> {
  await waitForItemsOnDisk(env, [...NODE_IDS], rows, 30_000)
  for (const nodeId of NODE_IDS) {
    const stored = new Map(readItems(dbPathFor(env, nodeId)).map(row => [row.id, row]))
    for (const row of rows) {
      expect(stored.get(row.id), `${nodeId} lost acknowledged row ${row.id}`).toEqual(row)
    }
  }
}

async function moveCopy(env: GateEnvironment, nodeId: string, others: readonly string[]): Promise<void> {
  await setDraining(env, nodeId, true)
  await waitForGroup(
    env,
    group => group.currentPrimary?.nodeId !== nodeId && !group.inSyncNodeIds.includes(nodeId),
    30_000,
    `${nodeId} drained and holding no primary duty`,
  )
  await killNode(env, nodeId)
  await startEmptyNode(env, nodeId)
  const moved = requireNode(env, nodeId)
  await waitForNodeReady(moved, 60_000)
  await setDraining(env, nodeId, false)
  await waitForGroup(
    env,
    group => others.every(other => group.inSyncNodeIds.includes(other)) && group.inSyncNodeIds.includes(nodeId),
    60_000,
    `${nodeId} moved and back in the in-sync set`,
  )
}

describe('moving one copy of a database to an empty disk under the same node ID', () => {
  const environment = useFailoverEnvironment()

  it('never promotes a copy that restarts with an empty file while the group takes no writes', async () => {
    const env = environment()
    await startCluster(env)
    const written = await writeRows(env, NODE_A, [1, 2, 3])
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    await killNode(env, NODE_C)
    await setReplicationLinks(env, NODE_C, false, [NODE_A])
    await startEmptyNode(env, NODE_C)
    await waitForCondition(async () => (await env.etcdOracle.nodeSession(NODE_C)) !== null, 15_000)
    expect(readItems(dbPathFor(env, NODE_C))).toEqual([])

    await setDraining(env, NODE_B, true)
    await killNode(env, NODE_A)
    await sleep(8_000)
    await setDraining(env, NODE_B, false)
    await setReplicationLinks(env, NODE_C, true, [NODE_A])
    await sleep(15_000)

    const group = await env.etcdOracle.group()
    const promoted = await statusOf(requireNode(env, NODE_C))
    const write = await requireNode(env, NODE_B)
      .execute('INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)', [9, NODE_B, 9, 'after'], {
        writeConcern: { level: 'majority', timeoutMs: 3_000 },
      })
      .then(
        () => 'accepted',
        (err: unknown) => String(err),
      )
    expect.soft(readItems(dbPathFor(env, NODE_B)), 'B rows').toEqual(written)
    expect
      .soft(readItems(dbPathFor(env, NODE_C)), `C rows; C phase ${promoted.syncState?.phase}; write ${write}`)
      .toEqual(written)
    expect(group?.currentPrimary?.nodeId, 'the empty copy took primary duty').not.toBe(NODE_C)
  })

  it('keeps a copy out of the in-sync set until its full copy finishes, while the group takes no writes', async () => {
    const env = environment()
    await startCluster(env)
    await writeRows(env, NODE_A, [1, 2, 3])
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    await killNode(env, NODE_C)
    for (const proxy of [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]) {
      await env.toxiproxy.addLatency(proxy, 'slow-copy', 1_500)
    }
    await startEmptyNode(env, NODE_C)
    const copying = requireNode(env, NODE_C)

    const violations: string[] = []
    const deadline = Date.now() + 25_000
    while (Date.now() < deadline) {
      const status = await statusOf(copying)
      const group = await env.etcdOracle.group()
      const phase = status.syncState?.phase
      if (phase === 'ready') break
      if (group?.inSyncNodeIds.includes(NODE_C)) {
        violations.push(`in-sync while ${phase}, holding ${readItems(dbPathFor(env, NODE_C)).length} rows`)
      }
      await sleep(250)
    }
    expect(violations).toEqual([])
  })

  it('moves each copy in turn, the primary included, under write load and loses no acknowledged write', async () => {
    const env = environment()
    await startCluster(env)
    const before = await writeRows(env, NODE_A, [1, 2, 3])
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    const load = startWriteLoad(env, 1_000)
    try {
      await moveCopy(env, NODE_C, [NODE_A, NODE_B])
      await moveCopy(env, NODE_B, [NODE_A, NODE_C])
      await moveCopy(env, NODE_A, [NODE_B, NODE_C])
      await sleep(2_000)
    } finally {
      await load.stop()
    }
    expect(load.acknowledged.length).toBeGreaterThan(20)
    await waitForPrimary(env, [NODE_B, NODE_C], 10_000)
    await expectAcknowledgedRowsEverywhere(env, [...before, ...load.acknowledged])
  })

  it('loses no acknowledged write when a remaining copy dies mid-move and the moving copy restarts mid-copy', async () => {
    const env = environment()
    await startCluster(env)
    const before = await writeRows(env, NODE_A, [1, 2, 3])
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    const load = startWriteLoad(env, 1_000)
    try {
      await setDraining(env, NODE_C, true)
      await waitForGroup(env, group => !group.inSyncNodeIds.includes(NODE_C), 15_000, `${NODE_C} drained`)
      await killNode(env, NODE_C)
      for (const proxy of [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]) {
        await env.toxiproxy.addLatency(proxy, 'slow-copy', 800)
      }
      await startEmptyNode(env, NODE_C)
      await sleep(1_500)
      await killNode(env, NODE_C)
      await startNode(env, NODE_C, { role: 'replica' })
      await killNode(env, NODE_B)
      await sleep(3_000)
      await startNode(env, NODE_B, { role: 'replica' })
      for (const proxy of [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]) {
        await env.toxiproxy.clearToxic(proxy, 'slow-copy')
      }
      await waitForNodeReady(requireNode(env, NODE_C), 60_000)
      await setDraining(env, NODE_C, false)
      await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 60_000)
      await sleep(2_000)
    } finally {
      await load.stop()
    }
    expect(load.acknowledged.length).toBeGreaterThan(10)
    await expectAcknowledgedRowsEverywhere(env, [...before, ...load.acknowledged])
  })
})
