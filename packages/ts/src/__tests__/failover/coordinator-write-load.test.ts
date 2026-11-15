import { describe, expect, it } from 'vitest'
import { waitForInSyncSet } from './gate-cluster-state.js'
import { dbPathFor, NODE_A, NODE_IDS, requireNode } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { startCluster } from './gate-scenarios.js'
import { waitForCondition } from './gate-support.js'
import { readItemIds } from './sqlite-oracle.js'

const CONCURRENCY_LEVELS = [16, 64, 256] as const
const WRITES_PER_LEVEL = 1_000
const WRITE_TIMEOUT_MS = 15_000
const REPLY_TIMEOUT_MS = 60_000

interface FailedWrite {
  id: number
  error: string
}

async function writeConcurrently(
  write: (id: number) => Promise<unknown>,
  ids: readonly number[],
  concurrency: number,
): Promise<FailedWrite[]> {
  const failed: FailedWrite[] = []
  let cursor = 0
  const worker = async (): Promise<void> => {
    for (let id = ids[cursor++]; id !== undefined; id = ids[cursor++]) {
      try {
        await write(id)
      } catch (err: unknown) {
        failed.push({ id, error: err instanceof Error ? err.message : String(err) })
      }
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()))
  return failed
}

describe('coordinator mode under concurrent write load', () => {
  const environment = useFailoverEnvironment()

  it('acknowledges every majority write while every voter is live and current, and writes nothing to etcd for them', async () => {
    const env = environment()
    await startCluster(env)
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    const primary = requireNode(env, NODE_A)
    const versionBefore = await env.etcdOracle.groupRecordVersion()

    const written: number[] = []
    for (const concurrency of CONCURRENCY_LEVELS) {
      const ids = Array.from({ length: WRITES_PER_LEVEL }, (_, index) => written.length + index + 1)
      const failed = await writeConcurrently(
        id =>
          primary.execute(
            'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
            [id, NODE_A, id, `${concurrency}-in-flight`],
            { writeConcern: { level: 'majority', timeoutMs: WRITE_TIMEOUT_MS } },
            REPLY_TIMEOUT_MS,
          ),
        ids,
        concurrency,
      )
      expect(failed, `${failed.length} of ${WRITES_PER_LEVEL} writes at ${concurrency} in flight failed`).toEqual([])
      written.push(...ids)
    }

    expect(await env.etcdOracle.groupRecordVersion()).toBe(versionBefore)
    expect((await env.etcdOracle.group())?.inSyncNodeIds.sort()).toEqual([...NODE_IDS].sort())
    for (const nodeId of NODE_IDS) {
      await waitForCondition(async () => readItemIds(dbPathFor(env, nodeId)).length === written.length, 15_000)
      expect(readItemIds(dbPathFor(env, nodeId))).toEqual(written)
    }
  }, 300_000)
})
