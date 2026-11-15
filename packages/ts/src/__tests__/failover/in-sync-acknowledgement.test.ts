import { copyFileSync, rmSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'
import { waitForGroup, waitForInSyncSet } from './gate-cluster-state.js'
import {
  dbPathFor,
  type GateEnvironment,
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
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { type ItemRow, readItems } from './sqlite-oracle.js'

const SESSION_OUTLASTING_A_RESTART_MS = 10_000
const REPLICAS = [NODE_B, NODE_C]

async function writeRows(env: GateEnvironment, ids: readonly number[], note: string): Promise<ItemRow[]> {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, NODE_A, note)
    await writeItemMajority(requireNode(env, NODE_A), row)
    rows.push(row)
  }
  return rows
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

function replaceWithCopy(env: GateEnvironment, nodeId: string, copyPath: string): void {
  const dbPath = dbPathFor(env, nodeId)
  for (const suffix of ['', '-wal', '-shm', '-journal']) rmSync(`${dbPath}${suffix}`, { force: true })
  copyFileSync(copyPath, dbPath)
}

describe('restarting the last in-sync copy of a group', () => {
  const environment = useFailoverEnvironment()

  it('makes a restarted primary that lost acknowledged writes give up its duty, and keeps those writes on the replicas', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS } })
    const written = await writeRows(env, [1], 'held-by-the-earlier-file')
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    const earlierFile = snapshotFile(env, NODE_A)
    written.push(...(await writeRows(env, [2, 3], 'missing-from-the-earlier-file')))
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    for (const nodeId of REPLICAS) await setReplicationLinks(env, nodeId, false, [NODE_A])
    for (const nodeId of REPLICAS) {
      await killNode(env, nodeId)
      await startNode(env, nodeId, {
        role: 'replica',
        sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS,
        awaitReady: false,
      })
    }
    const alone = await waitForGroup(
      env,
      group => group.inSyncNodeIds.length === 1 && group.inSyncNodeIds[0] === NODE_A,
      20_000,
      `${NODE_A} as the only copy in the in-sync set`,
    )

    await killNode(env, NODE_A)
    replaceWithCopy(env, NODE_A, earlierFile)
    for (const nodeId of REPLICAS) await setReplicationLinks(env, nodeId, true, [NODE_A])
    const restarted = await startNode(env, NODE_A, {
      role: 'replica',
      sessionTtlMs: SESSION_OUTLASTING_A_RESTART_MS,
      awaitReady: false,
    })

    const gaveUp = await waitForGroup(
      env,
      group => group.repairingNodeIds.includes(NODE_A),
      30_000,
      `${NODE_A} marked itself repairing`,
    )
    expect(gaveUp.primaryTerm).toBe(alone.primaryTerm)
    await expect(writeItemMajority(restarted, itemRow(4, NODE_A, 'from-a-stale-copy'))).rejects.toThrow()
    for (const nodeId of REPLICAS) expect(readItems(dbPathFor(env, nodeId))).toEqual(written)
  })
})
