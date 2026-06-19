import { describe, expect, it, vi } from 'vitest'
import type { ReplicationEngine } from '../../engine/engine.js'
import { SyncJoiner } from '../../engine/sync-joiner.js'
import { SyncServer } from '../../engine/sync-server.js'
import type { ReplicationErrorEvent, SyncAck, SyncBatch, SyncComplete } from '../../types.js'

describe('initial sync protocol ordering', () => {
  it('arms the joiner before the sync request can deliver data', async () => {
    const events: string[] = []
    const engine = {
      tracker: {},
      nodeId: 'node-b',
      isCoordinatorMode: () => true,
      getCurrentPrimaryPeerId: () => 'node-a',
      config: {
        transport: {
          peers: () => new Map([['node-a', { role: 'primary' }]]),
          requestSync: async () => {
            events.push(`request:${engine.syncState.phase}`)
          },
        },
      },
      log: {
        getSyncState: async () => ({ phase: 'pending', completedTables: [] }),
        setSyncMeta: async (phase: string) => {
          events.push(`persist:${phase}`)
        },
      },
      syncState: {
        phase: 'pending',
        sourcePeerId: null,
        snapshotSeq: null,
        completedTables: [],
        totalTables: 0,
        startedAt: null,
        error: null,
      },
      decorateSyncRequest: <T>(request: T): T => request,
    } as unknown as ReplicationEngine

    const joiner = new SyncJoiner(engine)
    await Promise.all([joiner.initiateSync(), joiner.initiateSync()])

    expect(events).toEqual(['persist:syncing', 'request:syncing'])
    expect(engine.syncState.phase).toBe('syncing')
    expect(engine.syncState.sourcePeerId).toBe('node-a')
  })

  it('registers a batch ACK waiter before the batch can be acknowledged', async () => {
    const sentCompletes: SyncComplete[] = []
    const errors: ReplicationErrorEvent[] = []
    let server: SyncServer

    const connection = {
      exec: vi.fn(async () => undefined),
      prepare: vi.fn(async () => ({
        get: vi.fn(async () => ({ ready: 1 })),
      })),
      close: vi.fn(async () => undefined),
    }

    const engine = {
      nodeId: 'node-a',
      isCoordinatorMode: () => false,
      config: {
        topology: { canWrite: () => true },
        transport: {
          peers: () => new Map([['node-b', { role: 'replica' }]]),
          sendSyncBatch: async (_peerId: string, batch: SyncBatch) => {
            const ack: SyncAck = {
              requestId: batch.requestId,
              joinerNodeId: 'node-b',
              table: batch.table,
              batchIndex: batch.batchIndex,
              success: true,
            }
            server.handleSyncAckReceived(ack)
          },
          sendSyncComplete: async (_peerId: string, complete: SyncComplete) => {
            sentCompletes.push(complete)
          },
        },
      },
      snapshotConnectionFactory: async () => connection,
      log: {
        getLocalSeq: async () => 0n,
        registerActiveSyncSeq: vi.fn(),
        unregisterActiveSyncSeq: vi.fn(),
        getTablesInFkOrder: async () => [],
        dumpSchema: async () => [],
      },
      maxConcurrentSyncs: 2,
      maxSyncDurationMs: 1_000,
      syncAckTimeoutMs: 20,
      syncBatchSize: 100,
      decorateSyncBatch: <T>(batch: T): T => batch,
      decorateSyncComplete: <T>(complete: T): T => complete,
      emitError: (event: ReplicationErrorEvent) => {
        errors.push(event)
      },
    } as unknown as ReplicationEngine

    server = new SyncServer(engine)
    await server.handleSyncRequest(
      {
        requestId: 'sync-request-1',
        joinerNodeId: 'node-b',
        completedTables: [],
      },
      'node-b',
    )

    await vi.waitFor(() => {
      expect(sentCompletes).toHaveLength(1)
    })
    expect(errors).toEqual([])
  })
})
