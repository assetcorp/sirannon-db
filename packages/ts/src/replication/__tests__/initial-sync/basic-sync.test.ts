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

  describe('basic sync', () => {
    it('syncs a single table from primary to a new replica', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
      ])
      await primary.engine.start()

      for (let i = 1; i <= 50; i++) {
        await primary.engine.execute(`INSERT INTO users (id, name) VALUES (${i}, 'user_${i}')`)
      }

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM users')
      const row = (await stmt.get()) as { cnt: number }
      expect(row.cnt).toBe(50)

      const nameStmt = await replica.conn.prepare('SELECT name FROM users WHERE id = 25')
      const nameRow = (await nameStmt.get()) as { name: string }
      expect(nameRow.name).toBe('user_25')
    })

    it('syncs multi-table with FK in correct order', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
        'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), total INTEGER NOT NULL)',
        'CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), product TEXT NOT NULL)',
      ])
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO users VALUES (1, 'alice')")
      await primary.engine.execute("INSERT INTO users VALUES (2, 'bob')")
      await primary.engine.execute('INSERT INTO orders VALUES (10, 1, 500)')
      await primary.engine.execute('INSERT INTO orders VALUES (11, 2, 300)')
      await primary.engine.execute("INSERT INTO order_items VALUES (100, 10, 'widget')")
      await primary.engine.execute("INSERT INTO order_items VALUES (101, 11, 'gadget')")

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const usersStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM users')
      const usersRow = (await usersStmt.get()) as { cnt: number }
      expect(usersRow.cnt).toBe(2)

      const ordersStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM orders')
      const ordersRow = (await ordersStmt.get()) as { cnt: number }
      expect(ordersRow.cnt).toBe(2)

      const itemsStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM order_items')
      const itemsRow = (await itemsStmt.get()) as { cnt: number }
      expect(itemsRow.cnt).toBe(2)
    })

    it('syncs schema with indexes', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL, name TEXT NOT NULL)',
      ])
      await primary.conn.exec('CREATE INDEX idx_products_sku ON products (sku)')
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO products VALUES (1, 'SKU001', 'Widget')")

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const idxStmt = await replica.conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_products_sku'",
      )
      const idxRow = (await idxStmt.get()) as { name: string } | undefined
      expect(idxRow).toBeDefined()
      expect(idxRow?.name).toBe('idx_products_sku')
    })

    it('syncs pre-existing rows with integers beyond 2^53 and BLOBs without corruption', async () => {
      const primary = await ctx.createPrimary(NODE_P, [
        'CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB)',
      ])
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO ledgers VALUES (1, 9007199254740993, X'0001FFAB')")
      await primary.engine.execute('INSERT INTO ledgers VALUES (2, 9223372036854775807, NULL)')
      await primary.engine.execute('INSERT INTO ledgers VALUES (3, -9223372036854775808, NULL)')
      await primary.engine.execute('INSERT INTO ledgers VALUES (4, 42, NULL)')

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const textStmt = await replica.conn.prepare(
        'SELECT CAST(balance AS TEXT) AS balance_text FROM ledgers ORDER BY id',
      )
      const textRows = (await textStmt.all()) as { balance_text: string }[]
      expect(textRows.map(r => r.balance_text)).toEqual([
        '9007199254740993',
        '9223372036854775807',
        '-9223372036854775808',
        '42',
      ])

      const valueStmt = await replica.conn.prepare('SELECT balance, payload FROM ledgers WHERE id = 1')
      const valueRow = (await valueStmt.get()) as { balance: unknown; payload: Uint8Array }
      expect(valueRow.balance).toBe(9007199254740993n)
      expect(Buffer.compare(Buffer.from(valueRow.payload), Buffer.from([0x00, 0x01, 0xff, 0xab]))).toBe(0)
    })

    it('syncs empty database (schema only, no rows)', async () => {
      const primary = await ctx.createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      const replica = await ctx.createReplica(NODE_R)
      await replica.engine.start()

      await wait(1500)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await replica.conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'")
      const row = (await stmt.get()) as { name: string } | undefined
      expect(row).toBeDefined()
    })
  })
})
