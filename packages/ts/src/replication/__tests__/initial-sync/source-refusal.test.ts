import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationErrorEvent, SyncAck } from '../../types.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  NODE_A,
  NODE_B,
  teardownHarness,
} from '../engine/helpers.js'

function refusal(requestId: string, joinerNodeId: string, error: string): SyncAck {
  return { requestId, joinerNodeId, table: '__schema__', batchIndex: -1, success: false, error }
}

describe('sync source refusal', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  async function startJoiner(): Promise<{ engine: ReplicationEngine; errors: ReplicationErrorEvent[] }> {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_B,
      topology: new PrimaryReplicaTopology('replica'),
      transport: harness.transport,
      initialSync: true,
      changeTracker: new ChangeTracker(),
    })
    const errors: ReplicationErrorEvent[] = []
    engine.on('replication-error', event => {
      errors.push(event)
    })
    await engine.start()
    harness.transport.addPeer(NODE_A, 'primary')
    await vi.waitFor(() => expect(harness.transport.sentSyncRequests.length).toBe(1))
    return { engine, errors }
  }

  it('returns to pending and asks again after the source refuses the request', async () => {
    const { engine, errors } = await startJoiner()
    const first = harness.transport.sentSyncRequests[0]
    expect(engine.status().syncState?.phase).toBe('syncing')

    harness.transport.triggerSyncAckReceived(
      refusal(first.request.requestId, NODE_B, 'Sync capacity reached, retry later'),
      NODE_A,
    )

    await vi.waitFor(() => expect(engine.status().syncState?.phase).toBe('pending'))
    expect(errors.map(event => event.operation)).toContain('sync-refused')
    expect(engine.status().syncState?.error).toBe('Sync capacity reached, retry later')

    await vi.waitFor(() => expect(harness.transport.sentSyncRequests.length).toBe(2), { timeout: 5_000, interval: 25 })
    expect(harness.transport.sentSyncRequests[1].request.requestId).not.toBe(first.request.requestId)

    await engine.stop()
  })

  it('ignores a refusal from a peer that is not the sync source', async () => {
    const { engine } = await startJoiner()
    const first = harness.transport.sentSyncRequests[0]

    harness.transport.triggerSyncAckReceived(refusal(first.request.requestId, NODE_B, 'Duplicate requestId'), NODE_B)

    await new Promise(resolve => setTimeout(resolve, 100))
    expect(engine.status().syncState?.phase).toBe('syncing')
    expect(harness.transport.sentSyncRequests.length).toBe(1)

    await engine.stop()
  })

  it('ignores a refusal that does not match the outstanding request', async () => {
    const { engine } = await startJoiner()

    harness.transport.triggerSyncAckReceived(refusal('a-stale-request-id', NODE_B, 'Duplicate requestId'), NODE_A)

    await new Promise(resolve => setTimeout(resolve, 100))
    expect(engine.status().syncState?.phase).toBe('syncing')
    expect(harness.transport.sentSyncRequests.length).toBe(1)

    await engine.stop()
  })
})
