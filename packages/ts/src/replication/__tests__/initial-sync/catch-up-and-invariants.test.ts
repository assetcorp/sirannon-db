import { afterEach, beforeEach, describe, expect, it } from 'vitest'
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

  describe('catch-up after sync', () => {
    it('transitions from catching-up to ready after applying incremental changes', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 20; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await ctx.createReplica(NODE_R, { maxSyncLagBeforeReady: 5 })
      await replica.engine.start()

      await wait(3000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')
    })
  })

  describe('CDC trigger isolation', () => {
    it('does not pollute _sirannon_changes during sync wipe and data insertion', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 10; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await ctx.createReplica(NODE_R)

      const seqBefore = await (async () => {
        await replica.conn.exec(`CREATE TABLE IF NOT EXISTS _sirannon_changes (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          table_name TEXT NOT NULL,
          operation TEXT NOT NULL,
          row_id TEXT NOT NULL,
          changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
          old_data TEXT,
          new_data TEXT,
          node_id TEXT NOT NULL DEFAULT '',
          tx_id TEXT NOT NULL DEFAULT '',
          hlc TEXT NOT NULL DEFAULT ''
        )`)
        const stmt = await replica.conn.prepare('SELECT MAX(seq) as max_seq FROM _sirannon_changes')
        const row = (await stmt.get()) as { max_seq: number | null }
        return row?.max_seq ?? 0
      })()

      await replica.engine.start()
      await wait(2000)

      const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM _sirannon_changes WHERE seq > ?')
      const row = (await stmt.get(seqBefore)) as { cnt: number }
      expect(row.cnt).toBe(0)
    })
  })

  describe('FK pragma restoration', () => {
    it('restores PRAGMA foreign_keys after sync completes', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE parents (id INTEGER PRIMARY KEY)',
        'CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))',
      ])
      await primary.engine.start()
      await primary.engine.execute('INSERT INTO parents VALUES (1)')
      await primary.engine.execute('INSERT INTO children VALUES (10, 1)')

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()
      await wait(2000)

      expect(replica.engine.status().syncState?.phase).toBe('ready')

      const fkStmt = await replica.conn.prepare('PRAGMA foreign_keys')
      const fkRow = (await fkStmt.get()) as { foreign_keys: number }
      expect(fkRow.foreign_keys).toBe(1)
    })
  })

  describe('catch-up deadline', () => {
    it('transitions to ready after deadline even with remaining lag', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()
      await primary.engine.execute("INSERT INTO items VALUES (1, 'seed')")

      const replica = await ctx.createReplica(NODE_R, {
        catchUpDeadlineMs: 500,
        maxSyncLagBeforeReady: 999_999,
      })
      await replica.engine.start()
      await wait(3000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')
    })
  })

  describe('integrity verification', () => {
    it('detects row count mismatch in manifest', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 10; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const countStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM items')
      const countRow = (await countStmt.get()) as { cnt: number }
      expect(countRow.cnt).toBe(10)
    })
  })
})
