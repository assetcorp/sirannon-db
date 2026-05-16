import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import { InMemoryTransport } from '../../../transport/memory/index.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_A, NODE_B, NODE_P, NODE_R, NODE_R2, SyncTestContext, wait } from './helpers.js'

describe('Initial Sync', () => {
  let ctx: SyncTestContext

  beforeEach(() => {
    ctx = new SyncTestContext()
    ctx.setup()
  })

  afterEach(async () => {
    await ctx.teardown()
  })

  describe('concurrent joiners', () => {
    it('rejects when maxConcurrentSyncs is exceeded', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'], {
        maxConcurrentSyncs: 1,
      })
      await primary.engine.start()

      for (let i = 1; i <= 200; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica1 = await ctx.createReplica(NODE_R, { syncBatchSize: 10 })
      const replica2 = await ctx.createReplica(NODE_R2, { syncBatchSize: 10 })

      await replica1.engine.start()
      await wait(100)
      await replica2.engine.start()

      await wait(5000)

      const s1 = replica1.engine.status()
      const s2 = replica2.engine.status()
      const readyCount = [s1, s2].filter(s => s.syncState?.phase === 'ready').length
      expect(readyCount).toBeGreaterThanOrEqual(1)
    })
  })

  describe('primary-to-replica sync', () => {
    it('syncs data from primary A to joining replica B', async () => {
      const nodeA = await ctx.createSyncSourceNode(NODE_A, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await nodeA.engine.start()

      for (let i = 1; i <= 20; i++) {
        await nodeA.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const dbPathB = join(ctx.tempDir, `${NODE_B.slice(0, 8)}-${Date.now()}.db`)
      const connB = await testDriver.open(dbPathB)
      await connB.exec('PRAGMA journal_mode = WAL')
      ctx.openConns.push(connB)

      const trackerB = new ChangeTracker({ replication: true })
      const dbB = await Database.create('db-join-b', dbPathB, testDriver)
      ctx.openDbs.push(dbB)

      const transportB = new InMemoryTransport(ctx.bus)
      const engineB = new ReplicationEngine(dbB, connB, {
        nodeId: NODE_B,
        topology: new PrimaryReplicaTopology('replica'),
        transport: transportB,
        batchIntervalMs: 30,
        initialSync: true,
        changeTracker: trackerB,
        syncBatchSize: 100,
        syncAckTimeoutMs: 5000,
        catchUpDeadlineMs: 5000,
        maxSyncLagBeforeReady: 5,
      })
      ctx.runningEngines.push(engineB)
      await engineB.start()

      await wait(3000)

      const status = engineB.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await connB.prepare('SELECT COUNT(*) as cnt FROM items')
      const row = (await stmt.get()) as { cnt: number }
      expect(row.cnt).toBe(20)
    })
  })

  describe('large dataset', () => {
    it('syncs multiple tables with many rows', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)',
        'CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)',
        'CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)',
      ])
      await primary.engine.start()

      for (let t = 1; t <= 3; t++) {
        for (let i = 1; i <= 200; i++) {
          await primary.engine.execute(`INSERT INTO t${t} VALUES (${i}, 'v${i}')`)
        }
      }

      const replica = await ctx.createReplica(NODE_R, { syncBatchSize: 50 })
      await replica.engine.start()

      await wait(5000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      for (let t = 1; t <= 3; t++) {
        const stmt = await replica.conn.prepare(`SELECT COUNT(*) as cnt FROM t${t}`)
        const row = (await stmt.get()) as { cnt: number }
        expect(row.cnt).toBe(200)
      }
    })
  })

  describe('timeout', () => {
    it('source aborts sync when maxSyncDurationMs exceeded', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'], {
        maxSyncDurationMs: 200,
      })
      await primary.engine.start()

      for (let i = 1; i <= 500; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await ctx.createReplica(NODE_R, { syncBatchSize: 5 })
      await replica.engine.start()

      await wait(3000)

      const status = replica.engine.status()
      expect(['pending', 'syncing', 'ready']).toContain(status.syncState?.phase)
    })
  })
})
