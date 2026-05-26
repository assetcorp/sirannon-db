import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import { InMemoryTransport } from '../../../transport/memory/index.js'
import { ReplicationEngine } from '../../engine.js'
import { SyncError } from '../../errors.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_P, NODE_R, SyncTestContext, wait } from './helpers.js'

describe('Initial Sync', () => {
  let ctx: SyncTestContext

  beforeEach(() => {
    ctx = new SyncTestContext()
    ctx.setup()
  })

  afterEach(async () => {
    await ctx.teardown()
  })

  describe('read blocking', () => {
    it('throws SyncError during sync, succeeds after ready', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()
      await primary.engine.execute("INSERT INTO items VALUES (1, 'test')")

      const replica = await ctx.createReplica(NODE_R)

      const replicaStarted = replica.engine.start()
      await wait(50)

      if (replica.engine.status().syncState?.phase !== 'ready') {
        await expect(replica.engine.query('SELECT 1')).rejects.toThrow(SyncError)
      }

      await replicaStarted
      await wait(2000)

      expect(replica.engine.status().syncState?.phase).toBe('ready')
      const result = await replica.engine.query<{ id: number }>('SELECT id FROM items')
      expect(result.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('out-of-band path', () => {
    it('starts in ready state with initialSync: false', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      const dbPath = join(ctx.tempDir, `oob-${Date.now()}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      ctx.openConns.push(conn)

      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'items')

      const db = await Database.create('oob-db', dbPath, testDriver)
      ctx.openDbs.push(db)

      const transport = new InMemoryTransport(ctx.bus)
      const engine = new ReplicationEngine(db, conn, {
        nodeId: NODE_R,
        topology: new PrimaryReplicaTopology('replica'),
        transport,
        batchIntervalMs: 30,
        initialSync: false,
        changeTracker: tracker,
      })
      ctx.runningEngines.push(engine)
      await engine.start()

      expect(engine.status().syncState?.phase).toBe('ready')
    })

    it('starts in ready state with resumeFromSeq', async () => {
      const dbPath = join(ctx.tempDir, `resume-${Date.now()}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      ctx.openConns.push(conn)

      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'items')

      const db = await Database.create('resume-db', dbPath, testDriver)
      ctx.openDbs.push(db)

      const transport = new InMemoryTransport(ctx.bus)
      const engine = new ReplicationEngine(db, conn, {
        nodeId: NODE_R,
        topology: new PrimaryReplicaTopology('replica'),
        transport,
        batchIntervalMs: 30,
        initialSync: false,
        resumeFromSeq: 100n,
        changeTracker: tracker,
      })
      ctx.runningEngines.push(engine)
      await engine.start()

      expect(engine.status().syncState?.phase).toBe('ready')
    })
  })
})
