import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReplicationEngine } from '../../engine/engine.js'
import { SyncJoiner } from '../../engine/sync-joiner.js'
import { SyncServer } from '../../engine/sync-server.js'
import type { SyncBatch } from '../../types.js'
import { NODE_P, NODE_R, SyncTestContext, wait } from './helpers.js'

describe('initial sync totalTables', () => {
  describe('end to end', () => {
    let ctx: SyncTestContext

    beforeEach(() => {
      ctx = new SyncTestContext()
      ctx.setup()
    })

    afterEach(async () => {
      await ctx.teardown()
    })

    it('reports the full table count for a fresh multi-table sync', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
        'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), total INTEGER NOT NULL)',
        'CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), product TEXT NOT NULL)',
      ])
      await primary.engine.start()
      await primary.engine.execute("INSERT INTO users VALUES (1, 'alice')")
      await primary.engine.execute('INSERT INTO orders VALUES (10, 1, 500)')
      await primary.engine.execute("INSERT INTO order_items VALUES (100, 10, 'widget')")

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()
      await wait(2000)

      const syncState = replica.engine.status().syncState
      expect(syncState?.phase).toBe('ready')
      expect(syncState?.totalTables).toBe(3)
      expect(syncState?.completedTables.length).toBe(3)
    })

    it('reports a single table for an empty database (schema only, no rows)', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()
      await wait(1500)

      const syncState = replica.engine.status().syncState
      expect(syncState?.phase).toBe('ready')
      expect(syncState?.totalTables).toBe(1)
    })
  })

  describe('source schema batch', () => {
    it('carries the full table count even when the joiner has already completed tables', async () => {
      const allTables = ['users', 'orders', 'order_items']
      const sentBatches: SyncBatch[] = []
      let server: SyncServer

      const connection = {
        exec: vi.fn(async () => undefined),
        prepare: vi.fn(async () => ({ get: vi.fn(async () => ({ ready: 1 })) })),
        close: vi.fn(async () => undefined),
      }

      const engine = {
        nodeId: NODE_P,
        isCoordinatorMode: () => false,
        config: {
          topology: { canWrite: () => true },
          transport: {
            peers: () => new Map([[NODE_R, { role: 'replica' }]]),
            sendSyncBatch: async (_peerId: string, batch: SyncBatch) => {
              sentBatches.push(batch)
              server.handleSyncAckReceived({
                requestId: batch.requestId,
                joinerNodeId: NODE_R,
                table: batch.table,
                batchIndex: batch.batchIndex,
                success: true,
              })
            },
            sendSyncComplete: async () => undefined,
          },
        },
        snapshotConnectionFactory: async () => connection,
        log: {
          getLocalSeq: async () => 0n,
          registerActiveSyncSeq: vi.fn(),
          unregisterActiveSyncSeq: vi.fn(),
          getTablesInFkOrder: async () => allTables,
          dumpSchema: async () => [],
          dumpTableOnConnection: async function* () {},
          generateManifest: async (_conn: unknown, table: string) => ({ table, rowCount: 0, pkHash: '' }),
        },
        maxConcurrentSyncs: 2,
        maxSyncDurationMs: 1_000,
        syncAckTimeoutMs: 200,
        syncBatchSize: 100,
        decorateSyncBatch: <T>(batch: T): T => batch,
        decorateSyncComplete: <T>(complete: T): T => complete,
        decorateSyncAck: <T>(ack: T): T => ack,
        emitError: vi.fn(),
      } as unknown as ReplicationEngine

      server = new SyncServer(engine)
      await server.handleSyncRequest({ requestId: 'sync-1', joinerNodeId: NODE_R, completedTables: ['users'] }, NODE_R)

      await vi.waitFor(() => {
        expect(sentBatches.some(b => b.table === '__schema__')).toBe(true)
      })

      const schemaBatch = sentBatches.find(b => b.table === '__schema__')
      expect(schemaBatch?.totalTables).toBe(3)
    })
  })

  describe('joiner validation', () => {
    function buildJoinerEngine(): { engine: ReplicationEngine; joiner: SyncJoiner } {
      const engine = {
        tracker: {},
        nodeId: NODE_R,
        writerConn: { exec: async () => undefined },
        expectedBatchIndex: new Map<string, number>(),
        config: {
          transport: { sendSyncAck: async () => undefined },
        },
        syncState: {
          phase: 'syncing',
          sourcePeerId: NODE_P,
          snapshotSeq: null,
          completedTables: [],
          totalTables: 0,
          startedAt: null,
          error: null,
        },
        decorateSyncAck: <T>(ack: T): T => ack,
        emitError: vi.fn(),
      } as unknown as ReplicationEngine
      return { engine, joiner: new SyncJoiner(engine) }
    }

    function schemaBatch(totalTables: number | undefined): SyncBatch {
      return {
        requestId: 'sync-1',
        table: '__schema__',
        batchIndex: 0,
        rows: [],
        checksum: '',
        isLastBatchForTable: true,
        totalTables,
      }
    }

    it('adopts a valid count from the schema batch', async () => {
      const { engine, joiner } = buildJoinerEngine()
      await joiner.handleSyncBatchReceived(schemaBatch(5), NODE_P)
      expect(engine.syncState.totalTables).toBe(5)
    })

    it('leaves the count at zero when an older source omits the field', async () => {
      const { engine, joiner } = buildJoinerEngine()
      await joiner.handleSyncBatchReceived(schemaBatch(undefined), NODE_P)
      expect(engine.syncState.totalTables).toBe(0)
    })

    it('ignores a negative or non-integer count from the wire', async () => {
      const negative = buildJoinerEngine()
      await negative.joiner.handleSyncBatchReceived(schemaBatch(-1), NODE_P)
      expect(negative.engine.syncState.totalTables).toBe(0)

      const fractional = buildJoinerEngine()
      await fractional.joiner.handleSyncBatchReceived(schemaBatch(2.5), NODE_P)
      expect(fractional.engine.syncState.totalTables).toBe(0)
    })
  })
})
