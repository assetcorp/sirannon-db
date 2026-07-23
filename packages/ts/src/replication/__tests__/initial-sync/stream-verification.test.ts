import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReplicationEngine } from '../../engine/engine.js'
import { SyncJoiner } from '../../engine/sync-joiner.js'
import { SyncServer } from '../../engine/sync-server.js'
import { advanceStreamDigest, matchesStreamDigest } from '../../engine/sync-verification.js'
import { canonicaliseForChecksum } from '../../log.js'
import type { ReplicationErrorEvent, SyncAck, SyncBatch, SyncComplete } from '../../types.js'
import { NODE_P, NODE_R, SyncTestContext, wait } from './helpers.js'

type AckDelayEngine = ReplicationEngine & { __setAckDelayMs: (ms: number) => void }

describe('stream digest verification', () => {
  it('produces order-sensitive digests over batch checksums', () => {
    const forward = advanceStreamDigest(advanceStreamDigest(undefined, 'aaa', 3), 'bbb', 2)
    const reversed = advanceStreamDigest(advanceStreamDigest(undefined, 'bbb', 2), 'aaa', 3)

    expect(forward.rowCount).toBe(5)
    expect(reversed.rowCount).toBe(5)
    expect(forward.digest).not.toBe(reversed.digest)

    expect(matchesStreamDigest({ table: 't', rowCount: 5, batchDigest: forward.digest }, forward)).toBe(true)
    expect(matchesStreamDigest({ table: 't', rowCount: 5, batchDigest: forward.digest }, reversed)).toBe(false)
    expect(matchesStreamDigest({ table: 't', rowCount: 4, batchDigest: forward.digest }, forward)).toBe(false)
    expect(matchesStreamDigest({ table: 't', rowCount: 5, batchDigest: forward.digest }, undefined)).toBe(false)
    expect(matchesStreamDigest({ table: 't', rowCount: 5, pkHash: 'x' }, forward)).toBe(false)
  })

  function createServerHarness() {
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

    const generateManifest = vi.fn(async (_conn: unknown, table: string) => ({
      table,
      rowCount: 2,
      pkHash: 'legacy-hash',
    }))

    async function* dumpTableOnConnection() {
      yield { rows: [{ id: 1 }, { id: 2 }], checksum: 'batch-checksum-0', isLast: true }
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
        getTablesInFkOrder: async () => ['users'],
        dumpSchema: async () => [],
        dumpTableOnConnection,
        generateManifest,
      },
      maxConcurrentSyncs: 2,
      maxSyncDurationMs: 1_000,
      syncAckTimeoutMs: 50,
      syncBatchSize: 100,
      decorateSyncBatch: <T>(batch: T): T => batch,
      decorateSyncComplete: <T>(complete: T): T => complete,
      emitError: (event: ReplicationErrorEvent) => {
        errors.push(event)
      },
    } as unknown as ReplicationEngine

    server = new SyncServer(engine)
    return { server, sentCompletes, errors, generateManifest }
  }

  it('builds manifests from the stream digests when the joiner advertises support', async () => {
    const harness = createServerHarness()
    await harness.server.handleSyncRequest(
      {
        requestId: 'sync-request-digest',
        joinerNodeId: 'node-b',
        completedTables: [],
        supportsStreamVerification: true,
      },
      'node-b',
    )

    await vi.waitFor(() => {
      expect(harness.sentCompletes).toHaveLength(1)
    })
    const manifest = harness.sentCompletes[0].manifests[0]
    expect(manifest.table).toBe('users')
    expect(manifest.rowCount).toBe(2)
    expect(manifest.batchDigest).toBe(advanceStreamDigest(undefined, 'batch-checksum-0', 2).digest)
    expect(manifest.pkHash).toBeUndefined()
    expect(harness.generateManifest).not.toHaveBeenCalled()
    expect(harness.errors).toEqual([])
  })

  it('falls back to legacy manifests when the joiner does not advertise support', async () => {
    const harness = createServerHarness()
    await harness.server.handleSyncRequest(
      {
        requestId: 'sync-request-legacy',
        joinerNodeId: 'node-b',
        completedTables: [],
      },
      'node-b',
    )

    await vi.waitFor(() => {
      expect(harness.sentCompletes).toHaveLength(1)
    })
    const manifest = harness.sentCompletes[0].manifests[0]
    expect(manifest.pkHash).toBe('legacy-hash')
    expect(manifest.batchDigest).toBeUndefined()
    expect(harness.generateManifest).toHaveBeenCalledTimes(1)
    expect(harness.errors).toEqual([])
  })

  function createJoinerHarness() {
    const sentAcks: SyncAck[] = []
    const engine = {
      nodeId: 'node-b',
      tracker: { unwatch: vi.fn(async () => undefined) },
      writerConn: {
        exec: vi.fn(async () => undefined),
        transaction: vi.fn(async () => undefined),
      },
      syncState: { phase: 'syncing', sourcePeerId: 'node-a', completedTables: [] },
      expectedBatchIndex: new Map<string, number>(),
      syncTableDigests: new Map(),
      log: { setSyncTableStatus: vi.fn(async () => undefined) },
      config: {
        transport: {
          sendSyncAck: async (_peerId: string, ack: SyncAck) => {
            sentAcks.push(ack)
          },
        },
      },
      decorateSyncAck: <T>(ack: T): T => ack,
      emitError: vi.fn(),
    } as unknown as ReplicationEngine
    return { engine, sentAcks, joiner: new SyncJoiner(engine) }
  }

  async function deliverRows(harness: ReturnType<typeof createJoinerHarness>, rows: Record<string, unknown>[]) {
    await harness.joiner.handleSyncBatchReceived(
      {
        requestId: 'sync-request-column-validation',
        table: 'accounts',
        batchIndex: 0,
        rows,
        checksum: createHash('sha256').update(canonicaliseForChecksum(rows)).digest('hex'),
        isLastBatchForTable: true,
      },
      'node-a',
    )
  }

  function expectRejected(harness: ReturnType<typeof createJoinerHarness>, message: string) {
    expect(harness.sentAcks).toHaveLength(1)
    expect(harness.sentAcks[0].success).toBe(false)
    expect(harness.sentAcks[0].error).toContain(message)
    expect(harness.engine.syncTableDigests.size).toBe(0)
    expect(harness.engine.writerConn.transaction).not.toHaveBeenCalled()
  }

  it('rejects a batch whose rows contain only invalid column names', async () => {
    const harness = createJoinerHarness()
    await deliverRows(harness, [{ 'not a valid column': 1 }])
    expectRejected(harness, 'Invalid column name')
  })

  it('rejects a batch mixing valid and invalid column names instead of dropping the invalid ones', async () => {
    const harness = createJoinerHarness()
    await deliverRows(harness, [{ id: 1, 'drop table': 2 }])
    expectRejected(harness, 'Invalid column name')
  })

  it('rejects a batch whose later rows have different columns than the first row', async () => {
    const harness = createJoinerHarness()
    await deliverRows(harness, [
      { id: 1, owner: 'a' },
      { id: 2, extra: 'b' },
    ])
    expectRejected(harness, 'Row columns do not match batch columns')
  })

  it('rejects a batch of rows with no columns at all', async () => {
    const harness = createJoinerHarness()
    await deliverRows(harness, [{}])
    expectRejected(harness, 'No columns in sync batch')
  })
})

describe('stream verification end to end', () => {
  let ctx: SyncTestContext

  beforeEach(() => {
    ctx = new SyncTestContext()
    ctx.setup()
  })

  afterEach(async () => {
    await ctx.teardown()
  })

  it('completes a sync whose total duration exceeds the per-progress deadline', async () => {
    const primary = await ctx.createPrimary(
      NODE_P,
      ['CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)'],
      { syncBatchSize: 10, maxSyncDurationMs: 400 },
    )
    await primary.engine.start()

    for (let i = 1; i <= 100; i++) {
      await primary.engine.execute(`INSERT INTO events (id, payload) VALUES (${i}, 'event_${i}')`)
    }

    const replica = await ctx.createReplica(NODE_R)
    ;(replica.engine as AckDelayEngine).__setAckDelayMs(70)
    await replica.engine.start()

    await wait(4000)

    expect(replica.engine.status().syncState?.phase).toBe('ready')

    const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM events')
    const row = (await stmt.get()) as { cnt: number }
    expect(row.cnt).toBe(100)
  })

  it('detects a corrupted stream digest, wipes, and resyncs to a correct state', async () => {
    const primary = await ctx.createPrimary(NODE_P, [
      'CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT NOT NULL)',
    ])
    await primary.engine.start()

    for (let i = 1; i <= 20; i++) {
      await primary.engine.execute(`INSERT INTO accounts (id, owner) VALUES (${i}, 'owner_${i}')`)
    }

    const originalSend = primary.transport.sendSyncComplete.bind(primary.transport)
    let completesSent = 0
    primary.transport.sendSyncComplete = async (peerId: string, complete: SyncComplete) => {
      completesSent += 1
      if (completesSent === 1) {
        const tampered: SyncComplete = {
          ...complete,
          manifests: complete.manifests.map(m => ({ ...m, batchDigest: '0'.repeat(64) })),
        }
        await originalSend(peerId, tampered)
        return
      }
      await originalSend(peerId, complete)
    }

    const replica = await ctx.createReplica(NODE_R)
    await replica.engine.start()

    await wait(4000)

    expect(completesSent).toBeGreaterThanOrEqual(2)
    expect(replica.engine.status().syncState?.phase).toBe('ready')

    const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM accounts')
    const row = (await stmt.get()) as { cnt: number }
    expect(row.cnt).toBe(20)

    const ownerStmt = await replica.conn.prepare('SELECT owner FROM accounts WHERE id = 7')
    const ownerRow = (await ownerStmt.get()) as { owner: string }
    expect(ownerRow.owner).toBe('owner_7')
  })
})
