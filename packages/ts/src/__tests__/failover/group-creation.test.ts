import { describe, expect, it } from 'vitest'
import type { StoredGroup } from './etcd-oracle.js'
import { waitForItemsOnDisk } from './gate-assertions.js'
import { waitForGroup } from './gate-cluster-state.js'
import {
  dbPathFor,
  type GateEnvironment,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  startNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForNodeReady } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { holdsThroughout } from './gate-support.js'
import { readMetaValue } from './sqlite-oracle.js'

const PRIMARY_TERM_START_KEY = 'replication_primary_term_start'

async function storedGroup(env: GateEnvironment): Promise<StoredGroup> {
  const group = await env.etcdOracle.group()
  if (!group) throw new Error(`etcd holds no group '${GROUP_ID}'`)
  return group
}

function recordedTermStart(env: GateEnvironment, nodeId: string): { term: string; seq: string } | null {
  const raw = readMetaValue(dbPathFor(env, nodeId), PRIMARY_TERM_START_KEY)
  return raw === null ? null : (JSON.parse(raw) as { term: string; seq: string })
}

async function isPrimaryInFirstTerm(env: GateEnvironment, nodeId: string): Promise<boolean> {
  const group = await env.etcdOracle.group()
  if (!group) return true
  return group.currentPrimary?.nodeId === nodeId && group.primaryTerm === 1n && !group.repairingNodeIds.includes(nodeId)
}

describe('creating a replication group', () => {
  const environment = useFailoverEnvironment()

  it('keeps primary duty for a primary that dies after creating its group and before its first write', async () => {
    const env = environment()
    const dying = await startNode(env, NODE_A, {
      role: 'primary',
      seedSchema: true,
      createsGroup: true,
      voters: NODE_IDS,
      dialTargets: [],
      dieAfterGroupCreate: true,
      awaitReady: false,
    })
    await dying.exited(30_000)
    await killNode(env, NODE_A)

    const created = await storedGroup(env)
    expect(created.currentPrimary?.nodeId).toBe(NODE_A)
    expect(created.primaryTerm).toBe(1n)
    expect(created.durabilityPointSeq).toBe(0n)
    expect(recordedTermStart(env, NODE_A)?.term).toBe('1')
    expect(await env.etcdOracle.nodeSession(NODE_A)).toBeNull()

    await Promise.all([
      startCluster(env),
      holdsThroughout(() => isPrimaryInFirstTerm(env, NODE_A), 10_000, `${NODE_A} to keep primary duty in term 1`),
    ])

    const restarted = await storedGroup(env)
    expect(restarted.currentPrimary?.nodeId).toBe(NODE_A)
    expect(restarted.primaryTerm).toBe(1n)
    expect(restarted.repairingNodeIds).toEqual([])
    const row = itemRow(1, NODE_A, 'first-write-after-restart')
    await writeItemMajority(requireNode(env, NODE_A), row)
    await waitForItemsOnDisk(env, NODE_IDS, [row], 20_000)
  })

  it('leaves no term start on the node that loses a race to create the group, and serves writes from the winner', async () => {
    const env = environment()
    const creators = [NODE_A, NODE_B]
    await Promise.all(
      creators.map(nodeId =>
        startNode(env, nodeId, {
          role: 'primary',
          seedSchema: true,
          createsGroup: true,
          voters: NODE_IDS,
          dialTargets: creators.filter(other => other !== nodeId),
          awaitReady: false,
        }),
      ),
    )
    await Promise.allSettled(creators.map(nodeId => requireNode(env, nodeId).ready()))

    const group = await storedGroup(env)
    const winner = group.currentPrimary?.nodeId
    expect(group.primaryTerm).toBe(1n)
    expect(creators).toContain(winner)
    const loser = creators.find(nodeId => nodeId !== winner)
    if (winner === undefined || loser === undefined) throw new Error('the race named no winner')

    expect(recordedTermStart(env, loser)).toBeNull()
    expect(recordedTermStart(env, winner)?.term).toBe('1')

    const replica = await startNode(env, NODE_C, { role: 'replica', dialTargets: creators })
    await waitForNodeReady(replica, 30_000)
    await waitForGroup(
      env,
      stored => stored.inSyncNodeIds.includes(winner) && stored.inSyncNodeIds.includes(NODE_C),
      30_000,
      `${winner} and ${NODE_C} in sync`,
    )
    const row = itemRow(1, winner, 'written-by-the-winner')
    await writeItemMajority(requireNode(env, winner), row)
    await waitForItemsOnDisk(env, [winner, NODE_C], [row], 20_000)
    expect((await storedGroup(env)).currentPrimary?.nodeId).toBe(winner)
    expect(recordedTermStart(env, loser)).toBeNull()
  })
})
