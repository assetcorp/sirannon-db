import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { HookDeniedError } from '../../errors.js'
import { Sirannon } from '../../sirannon.js'
import type { QueryHookContext } from '../../types.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Hooks integration', () => {
  describe('Sirannon-level hooks via options', () => {
    it('fires onBeforeQuery and onAfterQuery hooks from options', async () => {
      const beforeCalls: QueryHookContext[] = []
      const afterCalls: (QueryHookContext & { durationMs: number })[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onBeforeQuery: ctx => {
            beforeCalls.push(ctx)
          },
          onAfterQuery: ctx => {
            afterCalls.push(ctx)
          },
        },
      })

      const db = await sir.open('main', join(tempDir, 'hooks.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await db.query('SELECT * FROM users')

      expect(beforeCalls.length).toBeGreaterThanOrEqual(3)
      expect(afterCalls.length).toBeGreaterThanOrEqual(3)
      expect(beforeCalls[0].databaseId).toBe('main')
      expect(afterCalls[0].durationMs).toBeGreaterThanOrEqual(0)

      await sir.shutdown()
    })

    it('fires query hooks for every statement of a grouped transaction', async () => {
      const beforeCalls: QueryHookContext[] = []
      const afterCalls: (QueryHookContext & { durationMs: number })[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onBeforeQuery: ctx => {
            beforeCalls.push(ctx)
          },
          onAfterQuery: ctx => {
            afterCalls.push(ctx)
          },
        },
      })

      const db = await sir.open('main', join(tempDir, 'tx-hooks.db'))
      await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, ref TEXT)')
      beforeCalls.length = 0
      afterCalls.length = 0

      await db.executeTransaction([
        { sql: 'INSERT INTO orders (ref) VALUES (?)', params: ['first'] },
        { sql: 'INSERT INTO orders (ref) VALUES (?)', params: ['second'] },
      ])

      expect(beforeCalls.map(c => c.params)).toEqual([['first'], ['second']])
      expect(afterCalls.map(c => c.params)).toEqual([['first'], ['second']])
      expect(beforeCalls.every(c => c.databaseId === 'main')).toBe(true)
      expect(afterCalls.every(c => c.durationMs >= 0)).toBe(true)

      await sir.shutdown()
    })

    it('fires query hooks for a transaction that runs outside the group', async () => {
      const beforeCalls: QueryHookContext[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onBeforeQuery: ctx => {
            beforeCalls.push(ctx)
          },
        },
      })

      const db = await sir.open('main', join(tempDir, 'tx-ddl-hooks.db'))
      await db.executeTransaction([
        { sql: 'CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)' },
        { sql: 'INSERT INTO audit (note) VALUES (?)', params: ['opened'] },
      ])

      expect(beforeCalls.map(c => c.sql)).toEqual([
        'CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)',
        'INSERT INTO audit (note) VALUES (?)',
      ])

      await sir.shutdown()
    })

    it('fires onDatabaseOpen and onDatabaseClose hooks', async () => {
      const opened: string[] = []
      const closed: string[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onDatabaseOpen: ctx => {
            opened.push(ctx.databaseId)
          },
          onDatabaseClose: ctx => {
            closed.push(ctx.databaseId)
          },
        },
      })

      await sir.open('db1', join(tempDir, 'db1.db'))
      await sir.open('db2', join(tempDir, 'db2.db'))

      expect(opened).toEqual(['db1', 'db2'])
      expect(closed).toEqual([])

      await sir.close('db1')
      expect(closed).toEqual(['db1'])

      await sir.shutdown()
      expect(closed).toEqual(['db1', 'db2'])
    })

    it('fires onBeforeConnect hook and denies connection when hook throws', async () => {
      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onBeforeConnect: ctx => {
            if (ctx.databaseId === 'blocked') {
              throw new HookDeniedError('beforeConnect', 'access denied')
            }
          },
        },
      })

      await expect(sir.open('blocked', join(tempDir, 'blocked.db'))).rejects.toThrow(HookDeniedError)
      expect(sir.has('blocked')).toBe(false)

      const db = await sir.open('allowed', join(tempDir, 'allowed.db'))
      expect(db.id).toBe('allowed')

      await sir.shutdown()
    })
  })

  describe('Sirannon-level hooks via registration methods', () => {
    it('registers beforeQuery hook that can deny operations', async () => {
      const sir = new Sirannon({ driver: testDriver })
      sir.onBeforeQuery(ctx => {
        if (ctx.sql.includes('DROP')) {
          throw new HookDeniedError('beforeQuery', 'DROP statements are forbidden')
        }
      })

      const db = await sir.open('main', join(tempDir, 'hooks-reg.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')

      await expect(db.query('SELECT * FROM users')).resolves.not.toThrow()
      await expect(db.execute('DROP TABLE users')).rejects.toThrow(HookDeniedError)

      const rows = await db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
      expect(rows).toHaveLength(1)

      await sir.shutdown()
    })
  })

  describe('Database-level hooks', () => {
    it('registers per-database hooks independently', async () => {
      const sir = new Sirannon({ driver: testDriver })
      const db1 = await sir.open('db1', join(tempDir, 'db1-hooks.db'))
      const db2 = await sir.open('db2', join(tempDir, 'db2-hooks.db'))

      await db1.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)')
      await db2.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)')

      const db1Queries: string[] = []
      const db2Queries: string[] = []

      db1.onBeforeQuery(ctx => {
        db1Queries.push(ctx.sql)
      })
      db2.onBeforeQuery(ctx => {
        db2Queries.push(ctx.sql)
      })

      await db1.query('SELECT * FROM items')
      await db2.query('SELECT * FROM items')
      await db1.execute('INSERT INTO items (id) VALUES (1)')

      expect(db1Queries).toHaveLength(2)
      expect(db2Queries).toHaveLength(1)

      await sir.shutdown()
    })

    it('global hooks fire before local hooks', async () => {
      const order: string[] = []

      const sir = new Sirannon({ driver: testDriver })
      sir.onBeforeQuery(() => {
        order.push('global')
      })

      const db = await sir.open('main', join(tempDir, 'hook-order.db'))
      await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      order.length = 0

      db.onBeforeQuery(() => {
        order.push('local')
      })
      await db.query('SELECT * FROM t')

      expect(order).toEqual(['global', 'local'])

      await sir.shutdown()
    })
  })

  describe('standalone Database hooks (no Sirannon)', () => {
    it('local hooks work on standalone Database instances', async () => {
      const db = await Database.create('solo', join(tempDir, 'solo-hooks.db'), testDriver)
      await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      const queries: string[] = []
      db.onBeforeQuery(ctx => {
        queries.push(ctx.sql)
      })
      await db.query('SELECT * FROM t')

      expect(queries).toHaveLength(1)
      expect(queries[0]).toBe('SELECT * FROM t')

      await db.close()
    })

    it('beforeQuery hook can deny operations on standalone Database', async () => {
      const db = await Database.create('solo', join(tempDir, 'solo-deny.db'), testDriver)
      await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      db.onBeforeQuery(() => {
        throw new Error('denied')
      })

      await expect(db.query('SELECT * FROM t')).rejects.toThrow('denied')
      await db.close()
    })
  })
})
