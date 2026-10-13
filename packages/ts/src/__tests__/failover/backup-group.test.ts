import { describe, expect, it } from 'vitest'
import { storedBackupNames } from './directory-destination.js'
import { waitForGroup, waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  backupDirectoryFor,
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  serverBaseUrlFor,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { describeValue, waitForCondition } from './gate-support.js'

const SHARED_CHAIN_LIST_NAME = 'sirannon-backup-chain'

interface BackupStatus {
  running: boolean
  lastRun?: { destinationName: string; finishedAt: number }
  lastSkip?: { reason: string; preferredNodeId?: string }
  lastError?: { code: string; message: string }
}

async function backupStatusOf(env: GateEnvironment, nodeId: string): Promise<BackupStatus> {
  const response = await fetch(`${serverBaseUrlFor(env, nodeId)}/db/${GROUP_ID}/backup`, {
    signal: AbortSignal.timeout(5_000),
  })
  if (!response.ok) throw new Error(`Backup status on ${nodeId} returned ${response.status}: ${await response.text()}`)
  return (await response.json()) as BackupStatus
}

async function triggerBackup(env: GateEnvironment, nodeId: string): Promise<void> {
  const response = await fetch(`${serverBaseUrlFor(env, nodeId)}/db/${GROUP_ID}/backup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: '{}',
    signal: AbortSignal.timeout(10_000),
  })
  if (response.status !== 202) {
    throw new Error(`Backup trigger on ${nodeId} returned ${response.status}: ${await response.text()}`)
  }
}

async function runBackupTurn(env: GateEnvironment, liveNodeIds: readonly string[], expectedNodeId: string) {
  const before = new Set(await storedBackupNames(backupDirectoryFor(env)))
  const previousFinish = (await backupStatusOf(env, expectedNodeId)).lastRun?.finishedAt ?? 0
  for (const nodeId of liveNodeIds) {
    await triggerBackup(env, nodeId)
  }
  const statuses: Record<string, BackupStatus> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of liveNodeIds) {
        statuses[nodeId] = await backupStatusOf(env, nodeId)
      }
      return liveNodeIds.every(nodeId => {
        const status = statuses[nodeId]
        if (!status || status.running) return false
        if (nodeId === expectedNodeId) return (status.lastRun?.finishedAt ?? 0) > previousFinish
        return status.lastSkip?.reason === 'not-preferred' && status.lastSkip.preferredNodeId === expectedNodeId
      })
    }, 30_000)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    const group = await env.etcdOracle.group()
    throw new Error(
      `The backup turn never settled on ${expectedNodeId}: ${message}\n${describeValue(statuses)}\ngroup=${describeValue(group)}`,
    )
  }
  const written = (await storedBackupNames(backupDirectoryFor(env))).filter(
    name => !before.has(name) && name !== SHARED_CHAIN_LIST_NAME && !name.startsWith(`${SHARED_CHAIN_LIST_NAME}.`),
  )
  expect(written.length).toBeGreaterThan(0)
  for (const name of written) {
    expect(name.startsWith(`backup-${expectedNodeId}-`), `${name} was not written by ${expectedNodeId}`).toBe(true)
  }
}

describe('which node takes the backups of a group', () => {
  const environment = useFailoverEnvironment()

  it('takes each backup on one in-sync replica, moves off a drained replica, follows a failover, and falls back to the primary', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { backups: true } })
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'before-the-first-backup'))
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await runBackupTurn(env, NODE_IDS, NODE_B)

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_B,
      draining: true,
    })
    await waitForGroup(env, group => group.drainingNodeIds.includes(NODE_B), 5_000, `${NODE_B} draining`)
    await runBackupTurn(env, NODE_IDS, NODE_C)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_C], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    await requireNode(env, NODE_C).localWriteProbe(3, 'on-the-promoted-primary-before-its-backup')
    await runBackupTurn(env, [NODE_B, NODE_C], NODE_C)

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_B,
      draining: false,
    })
    await writeItemMajority(requireNode(env, NODE_C), itemRow(2, NODE_C, 'after-the-failover'))
    await waitForGroup(env, group => group.inSyncNodeIds.includes(NODE_B), 20_000, `${NODE_B} back in the in-sync set`)
    await runBackupTurn(env, [NODE_B, NODE_C], NODE_B)
  })
})
