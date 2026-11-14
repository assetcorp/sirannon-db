import { copyFileSync, rmSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'
import { waitForIdenticalFiles } from './gate-assertions.js'
import { waitForGroup, waitForInSyncSet } from './gate-cluster-state.js'
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
  setReplicationLinks,
  startNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForNodeReady } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { type ItemRow, readItems } from './sqlite-oracle.js'

const SESSION_OUTLASTING_A_RESTART_MS = 10_000
const MAX_REPLICA_LAG_MS = 1_000
const LAG_REMOVAL_ALLOWANCE_MS = 2_000
const MAX_GROUP_VOTERS = 7

async function writeRows(env: GateEnvironment, ids: readonly number[], note: string): Promise<ItemRow[]> {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, NODE_A, note)
    await writeItemMajority(requireNode(env, NODE_A), row)
    rows.push(row)
  }
  return rows
}

function replaceWithCopy(env: GateEnvironment, nodeId: string, copyPath: string): void {
  const dbPath = dbPathFor(env, nodeId)
  for (const suffix of ['', '-wal', '-shm', '-journal']) rmSync(`${dbPath}${suffix}`, { force: true })
  copyFileSync(copyPath, dbPath)
}

function snapshotFile(env: GateEnvironment, nodeId: string): string {
  const copyPath = `${dbPathFor(env, nodeId)}.earlier`
  rmSync(copyPath, { force: true })
  const source = new DatabaseSync(dbPathFor(env, nodeId), { readOnly: true })
  try {
    source.exec(`VACUUM INTO '${copyPath.replaceAll("'", "''")}'`)
  } finally {
    source.close()
  }
  return copyPath
}

describe('acknowledging writes against the in-sync set', () => {
  const environment = useFailoverEnvironment()

  it('keeps a restarted primary that holds every acknowledged write in a new term, and serves writes from it', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS } })
    const written = await writeRows(env, [1, 2, 3], 'before-the-restart')
    const before = await waitForInSyncSet(env, NODE_IDS, 15_000)

    await killNode(env, NODE_A)
    const restarted = await startNode(env, NODE_A, { role: 'replica', sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS })
    await waitForNodeReady(restarted, 30_000)
    const kept = await waitForGroup(
      env,
      group => group.currentPrimary?.nodeId === NODE_A && group.primaryTerm > before.primaryTerm,
      20_000,
      `${NODE_A} kept primary duty in a new term`,
    )
    expect(kept.primaryTerm).toBe(before.primaryTerm + 1n)
    expect(kept.repairingNodeIds).toEqual([])

    written.push(...(await writeRows(env, [4], 'after-the-restart')))
    expect(await waitForIdenticalFiles(env, NODE_IDS, 20_000)).toEqual(written)
  })

  it('makes a restarted primary whose file lost acknowledged writes give up its duty, and loses none of those writes', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS } })
    const written = await writeRows(env, [1], 'held-by-the-earlier-file')
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    const earlierFile = snapshotFile(env, NODE_A)
    written.push(...(await writeRows(env, [2, 3], 'missing-from-the-earlier-file')))
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await killNode(env, NODE_A)
    replaceWithCopy(env, NODE_A, earlierFile)
    expect(readItems(dbPathFor(env, NODE_A))).toEqual(written.slice(0, 1))
    const restarted = await startNode(env, NODE_A, {
      role: 'replica',
      sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS,
      awaitReady: false,
    })

    const handedOver = await waitForGroup(
      env,
      group => group.currentPrimary !== null && group.currentPrimary.nodeId !== NODE_A,
      30_000,
      'another node took primary duty from the primary that lost acknowledged writes',
    )
    const promoted = handedOver.currentPrimary?.nodeId ?? ''
    expect(readItems(dbPathFor(env, promoted))).toEqual(written)
    await waitForNodeReady(restarted, 60_000)
    const afterHandover = itemRow(4, promoted, 'after-the-handover')
    await writeItemMajority(requireNode(env, promoted), afterHandover)
    written.push(afterHandover)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 30_000)).toEqual(written)
  })

  it('removes a stalled in-sync replica within the lag limit, so that a majority write waits no longer than that', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { maxReplicaLagMs: MAX_REPLICA_LAG_MS } })
    await writeRows(env, [1], 'before-the-stall')
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await setReplicationLinks(env, NODE_C, false)
    const startedMs = Date.now()
    await writeRows(env, [2], 'while-c-stalls')
    const waitedMs = Date.now() - startedMs
    expect(waitedMs).toBeLessThan(MAX_REPLICA_LAG_MS + LAG_REMOVAL_ALLOWANCE_MS)
    const after = await env.etcdOracle.group()
    expect(after?.inSyncNodeIds.sort()).toEqual([NODE_A, NODE_B].sort())

    await setReplicationLinks(env, NODE_B, false)
    await expect(
      requireNode(env, NODE_A).execute(
        'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        [3, NODE_A, 3, 'without-a-majority'],
        { writeConcern: { level: 'majority', timeoutMs: 3_000 } },
      ),
    ).rejects.toThrow(/Timed out/)
    expect((await env.etcdOracle.group())?.inSyncNodeIds.sort()).toEqual([NODE_A, NODE_B].sort())
  })

  it('refuses a group of more than seven voters, and a feature level that a member does not support', async () => {
    const env = environment()
    const voters = Array.from({ length: MAX_GROUP_VOTERS + 1 }, (_, index) => `capped-voter-${index + 1}`)
    await expect(
      env.coordinator.createReplicationGroupState({
        clusterId: CLUSTER_ID,
        groupId: 'too-many-voters',
        votingDataBearingNodeIds: voters,
        currentPrimary: null,
        primaryTerm: 1n,
        inSyncNodeIds: [],
      }),
    ).rejects.toThrow(RangeError)
    const full = await env.coordinator.createReplicationGroupState({
      clusterId: CLUSTER_ID,
      groupId: 'seven-voters',
      votingDataBearingNodeIds: voters.slice(0, MAX_GROUP_VOTERS),
      currentPrimary: null,
      primaryTerm: 1n,
      inSyncNodeIds: [],
    })
    expect(full.created).toBe(true)
    await expect(
      env.coordinator.addVoter({
        clusterId: CLUSTER_ID,
        groupId: 'seven-voters',
        nodeId: voters[MAX_GROUP_VOTERS] ?? '',
        expectedVoterNodeIds: voters.slice(0, MAX_GROUP_VOTERS),
      }),
    ).rejects.toThrow(RangeError)

    await startCluster(env)
    await expect(
      env.coordinator.raiseFeatureLevel({ clusterId: CLUSTER_ID, groupId: GROUP_ID, featureLevel: 3 }),
    ).rejects.toThrow(RangeError)
    await expect(
      env.coordinator.raiseFeatureLevel({ clusterId: CLUSTER_ID, groupId: GROUP_ID, featureLevel: 1 }),
    ).rejects.toThrow(RangeError)
    expect((await env.etcdOracle.groupRecord(GROUP_ID))?.featureLevel).toBe(2)

    const record = await env.etcdOracle.groupRecord(GROUP_ID)
    await env.etcdOracle.writeGroupRecord(GROUP_ID, JSON.stringify({ ...record, featureLevel: 3 }))
    await killNode(env, NODE_B)
    const refused = await startNode(env, NODE_B, { role: 'replica', awaitReady: false })
    const failure = await refused.startupFailure()
    expect(failure.code).toBe('PROTOCOL_VERSION_MISMATCH')
    expect(refused.running).toBe(false)
  })
})
