import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { HookDeniedError, ReadOnlyError, SirannonError } from '../errors.js'
import { Sirannon } from '../sirannon.js'
import type { ChangeEvent, QueryHookContext } from '../types.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('CDC integration via Database', () => {
  it('watch -> insert -> on -> subscribe -> verify event fires', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    await db.watch('users')
    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('insert')
    expect(received[0].table).toBe('users')
    expect(received[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })
    expect(received[0].oldRow).toBeUndefined()

    await db.close()
  })

  it('captures UPDATE events with old and new row data', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-update.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await new Promise(resolve => setTimeout(resolve, 120))
    received.length = 0

    await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('update')
    expect(received[0].row.age).toBe(31)
    expect(received[0].oldRow?.age).toBe(30)

    await db.close()
  })

  it('captures DELETE events with old row data', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-delete.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise(resolve => setTimeout(resolve, 120))
    received.length = 0

    await db.execute("DELETE FROM users WHERE name = 'Alice'")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('delete')
    expect(received[0].oldRow?.name).toBe('Alice')

    await db.close()
  })

  it('filters subscriptions to matching events only', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-filter.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const aliceOnly: ChangeEvent[] = []
    const allEvents: ChangeEvent[] = []

    db.on('users')
      .filter({ name: 'Alice' })
      .subscribe(event => aliceOnly.push(event))
    db.on('users').subscribe(event => allEvents.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(allEvents).toHaveLength(3)
    expect(aliceOnly).toHaveLength(2)

    await db.close()
  })

  it('unwatch stops capturing new changes', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-unwatch.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise(resolve => setTimeout(resolve, 120))
    expect(received).toHaveLength(1)

    await db.unwatch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received).toHaveLength(1)

    await db.close()
  })

  it('throws ReadOnlyError when watching on a read-only database', async () => {
    const dbPath = join(tempDir, 'ro-cdc.db')
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await setup.close()

    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    await expect(db.watch('users')).rejects.toThrow(ReadOnlyError)
    await db.close()
  })

  it('supports creating subscriptions before watching', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-sub-first.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.name).toBe('Alice')

    await db.close()
  })

  it('respects custom CDC poll interval', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-interval.db'), testDriver, { cdcPollInterval: 20 })
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 80))

    expect(received.length).toBeGreaterThanOrEqual(1)
    await db.close()
  })

  it('cleans up CDC polling on close', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-close.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    db.on('users').subscribe(() => {})

    await db.close()

    await new Promise(resolve => setTimeout(resolve, 120))
    expect(db.closed).toBe(true)
  })
})

describe('Hooks integration', () => {
  describe('Sirannon-level hooks via options', () => {
    it('fires onBeforeQuery and onAfterQuery hooks from options', async () => {
      const beforeCalls: QueryHookContext[] = []
      const afterCalls: (QueryHookContext & { durationMs: number })[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onBeforeQuery: ctx => beforeCalls.push(ctx),
          onAfterQuery: ctx => afterCalls.push(ctx),
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

    it('fires onDatabaseOpen and onDatabaseClose hooks', async () => {
      const opened: string[] = []
      const closed: string[] = []

      const sir = new Sirannon({
        driver: testDriver,
        hooks: {
          onDatabaseOpen: ctx => opened.push(ctx.databaseId),
          onDatabaseClose: ctx => closed.push(ctx.databaseId),
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

      db1.onBeforeQuery(ctx => db1Queries.push(ctx.sql))
      db2.onBeforeQuery(ctx => db2Queries.push(ctx.sql))

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
      sir.onBeforeQuery(() => order.push('global'))

      const db = await sir.open('main', join(tempDir, 'hook-order.db'))
      await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      order.length = 0

      db.onBeforeQuery(() => order.push('local'))
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
      db.onBeforeQuery(ctx => queries.push(ctx.sql))
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

describe('Metrics integration', () => {
  it('tracks query metrics through MetricsConfig', async () => {
    const queryMetrics: { sql: string; durationMs: number }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onQueryComplete: m => queryMetrics.push({ sql: m.sql, durationMs: m.durationMs }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'metrics.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.query('SELECT * FROM users')

    expect(queryMetrics.length).toBeGreaterThanOrEqual(3)
    expect(queryMetrics.every(m => m.durationMs >= 0)).toBe(true)

    await sir.shutdown()
  })

  it('tracks connection open and close metrics', async () => {
    const connectionEvents: { databaseId: string; event: 'open' | 'close' }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onConnectionOpen: m => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
        onConnectionClose: m => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
      },
    })

    await sir.open('db1', join(tempDir, 'metrics-conn1.db'))
    await sir.open('db2', join(tempDir, 'metrics-conn2.db'))

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
    ])

    await sir.close('db1')

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
      { databaseId: 'db1', event: 'close' },
    ])

    await sir.shutdown()
  })

  it('tracks errors in query metrics', async () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onQueryComplete: m => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'metrics-err.db'))

    try {
      await db.query('SELECT * FROM nonexistent_table')
    } catch {
      /* expected */
    }

    expect(metrics.length).toBe(1)
    expect(metrics[0].error).toBe(true)

    await sir.shutdown()
  })
})

describe('Lifecycle integration', () => {
  it('auto-opens databases via resolver on resolve()', async () => {
    const dbPath = join(tempDir, 'auto-open.db')
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await setup.execute("INSERT INTO users (name) VALUES ('Alice')")
    await setup.close()

    const sir = new Sirannon({
      driver: testDriver,
      lifecycle: {
        autoOpen: {
          resolver: id => {
            if (id === 'auto') return { path: dbPath }
            return undefined
          },
        },
      },
    })

    expect(sir.has('auto')).toBe(false)

    const db = await sir.resolve('auto')
    expect(db).toBeDefined()
    expect(db?.id).toBe('auto')
    expect(sir.has('auto')).toBe(true)

    const rows = await db?.query<{ name: string }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows?.[0].name).toBe('Alice')

    expect(await sir.resolve('unknown')).toBeUndefined()

    await sir.shutdown()
  })

  it('closes idle databases after timeout', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      lifecycle: {
        autoOpen: {
          resolver: id => ({ path: join(tempDir, `${id}.db`) }),
        },
        idleTimeout: 200,
      },
    })

    const db = await sir.resolve('idle-test')
    expect(db).toBeDefined()
    expect(sir.has('idle-test')).toBe(true)

    await new Promise(resolve => setTimeout(resolve, 400))

    expect(sir.has('idle-test')).toBe(false)

    await sir.shutdown()
  })

  it('evicts LRU database when maxOpen is reached', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      lifecycle: {
        autoOpen: {
          resolver: id => ({ path: join(tempDir, `${id}.db`) }),
        },
        maxOpen: 2,
      },
    })

    await sir.resolve('db1')
    await sir.resolve('db2')

    expect(sir.databases().size).toBe(2)

    await sir.resolve('db3')

    expect(sir.databases().size).toBe(2)
    expect(sir.has('db1')).toBe(false)
    expect(sir.has('db2')).toBe(true)
    expect(sir.has('db3')).toBe(true)

    await sir.shutdown()
  })

  it('get() returns undefined after shutdown instead of throwing', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      lifecycle: {
        autoOpen: {
          resolver: () => ({ path: join(tempDir, 'post-shutdown.db') }),
        },
      },
    })

    await sir.shutdown()

    expect(sir.get('anything')).toBeUndefined()
  })

  it('lifecycle hooks and metrics fire for auto-opened databases', async () => {
    const opened: string[] = []
    const connectionEvents: string[] = []

    const sir = new Sirannon({
      driver: testDriver,
      hooks: {
        onDatabaseOpen: ctx => opened.push(ctx.databaseId),
      },
      metrics: {
        onConnectionOpen: m => connectionEvents.push(m.databaseId),
      },
      lifecycle: {
        autoOpen: {
          resolver: id => ({ path: join(tempDir, `${id}-lifecycle.db`) }),
        },
      },
    })

    await sir.resolve('auto-hooks')

    expect(opened).toEqual(['auto-hooks'])
    expect(connectionEvents).toEqual(['auto-hooks'])

    await sir.shutdown()
  })
})

describe('Backup integration via Database', () => {
  it('creates a one-shot backup via backup()', async () => {
    const db = await Database.create('test', join(tempDir, 'backup-source.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const backupPath = join(tempDir, 'backup-copy.db')
    await db.backup(backupPath)
    await db.close()

    const verify = await Database.create('verify', backupPath, testDriver)
    const rows = await verify.query<{ name: string }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    await verify.close()
  })

  it('throws when backing up a read-only database (no writer)', async () => {
    const dbPath = join(tempDir, 'ro-backup.db')
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    await setup.close()

    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    await expect(db.backup(join(tempDir, 'fail.db'))).rejects.toThrow()
    await db.close()
  })

  it('integrates backup through Sirannon', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'sir-backup.db'))
    await db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
    await db.execute("INSERT INTO products (title) VALUES ('Widget')")

    const backupPath = join(tempDir, 'sir-backup-copy.db')
    await db.backup(backupPath)

    const verify = await Database.create('verify', backupPath, testDriver)
    const rows = await verify.query<{ title: string }>('SELECT * FROM products')
    expect(rows).toHaveLength(1)
    expect(rows[0].title).toBe('Widget')
    await verify.close()

    await sir.shutdown()
  })
})

describe('Extension loading via Database', () => {
  it('throws ExtensionError for nonexistent extension path', async () => {
    const db = await Database.create('test', join(tempDir, 'ext.db'), testDriver)

    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow(SirannonError)
    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow('Failed to load extension')

    await db.close()
  })
})

describe('End-to-end smoke test', () => {
  it('open -> create table -> watch -> subscribe -> insert -> verify event -> query -> backup -> close', async () => {
    const queryLog: string[] = []
    const connectionLog: string[] = []

    const sir = new Sirannon({
      driver: testDriver,
      hooks: {
        onBeforeQuery: ctx => queryLog.push(ctx.sql),
      },
      metrics: {
        onConnectionOpen: m => connectionLog.push(`open:${m.databaseId}`),
        onConnectionClose: m => connectionLog.push(`close:${m.databaseId}`),
      },
    })

    const db = await sir.open('e2e', join(tempDir, 'e2e.db'))
    expect(connectionLog).toContain('open:e2e')

    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await db.watch('users')

    const events: ChangeEvent[] = []
    const sub = db.on('users').subscribe(event => events.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].type).toBe('insert')
    expect(events[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })

    sub.unsubscribe()

    const rows = await db.query<{ id: number; name: string; age: number }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')

    const backupPath = join(tempDir, 'e2e-backup.db')
    await db.backup(backupPath)

    const verify = await Database.create('verify', backupPath, testDriver)
    const backupRows = await verify.query<{ name: string }>('SELECT * FROM users')
    expect(backupRows).toHaveLength(1)
    expect(backupRows[0].name).toBe('Alice')
    await verify.close()

    expect(queryLog.length).toBeGreaterThan(0)

    await sir.shutdown()
    expect(connectionLog).toContain('close:e2e')
  })
})

describe('Combined integration edge cases', () => {
  it('beforeQuery hook denial prevents metrics from recording a successful query', async () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      hooks: {
        onBeforeQuery: ctx => {
          if (ctx.sql.includes('forbidden')) {
            throw new HookDeniedError('beforeQuery', 'forbidden query')
          }
        },
      },
      metrics: {
        onQueryComplete: m => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'hook-deny-metrics.db'))
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    await expect(db.query('SELECT forbidden FROM t')).rejects.toThrow(HookDeniedError)

    const forbiddenMetrics = metrics.filter(m => m.sql.includes('forbidden'))
    expect(forbiddenMetrics).toHaveLength(0)

    await sir.shutdown()
  })

  it('afterQuery hook errors do not mask query errors', async () => {
    const db = await Database.create('test', join(tempDir, 'after-mask.db'), testDriver)
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    await expect(db.query('INVALID SQL')).rejects.toThrow('syntax error')

    await db.close()
  })

  it('afterQuery hook errors do not mask successful query results', async () => {
    const db = await Database.create('test', join(tempDir, 'after-no-mask.db'), testDriver)
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    await db.execute("INSERT INTO t (val) VALUES ('hello')")

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    const rows = await db.query<{ val: string }>('SELECT * FROM t')
    expect(rows).toHaveLength(1)
    expect(rows[0].val).toBe('hello')

    await db.close()
  })

  it('multiple databases share global hooks but have independent local hooks', async () => {
    const globalLog: string[] = []
    const localLog1: string[] = []
    const localLog2: string[] = []

    const sir = new Sirannon({ driver: testDriver })
    sir.onBeforeQuery(ctx => globalLog.push(`${ctx.databaseId}:${ctx.sql}`))

    const db1 = await sir.open('db1', join(tempDir, 'multi-hook1.db'))
    const db2 = await sir.open('db2', join(tempDir, 'multi-hook2.db'))

    await db1.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    await db2.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db1.onBeforeQuery(ctx => localLog1.push(ctx.sql))
    db2.onBeforeQuery(ctx => localLog2.push(ctx.sql))

    globalLog.length = 0

    await db1.query('SELECT 1')
    await db2.query('SELECT 2')

    expect(globalLog).toEqual(['db1:SELECT 1', 'db2:SELECT 2'])
    expect(localLog1).toEqual(['SELECT 1'])
    expect(localLog2).toEqual(['SELECT 2'])

    await sir.shutdown()
  })
})

describe('CDC through Sirannon', () => {
  it('watch and subscribe work on databases opened through Sirannon', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'sir-cdc.db'))
    await db.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)')
    await db.watch('messages')

    const received: ChangeEvent[] = []
    db.on('messages').subscribe(event => received.push(event))

    await db.execute("INSERT INTO messages (body) VALUES ('hello world')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.body).toBe('hello world')

    await sir.shutdown()
  })
})

describe('Close listener cleanup', () => {
  it('lifecycle manager untracks databases when they close', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      lifecycle: {
        autoOpen: {
          resolver: id => ({ path: join(tempDir, `${id}-cleanup.db`) }),
        },
        idleTimeout: 60_000,
      },
    })

    const db = await sir.resolve('tracked')
    expect(db).toBeDefined()
    expect(sir.has('tracked')).toBe(true)

    await db?.close()

    expect(sir.has('tracked')).toBe(false)
  })
})
