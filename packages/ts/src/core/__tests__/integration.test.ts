import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { HookDeniedError, ReadOnlyError, SirannonError } from '../errors.js'
import { Sirannon } from '../sirannon.js'
import type { ChangeEvent, QueryHookContext } from '../types.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('CDC integration via Database', () => {
  it('watch -> insert -> on -> subscribe -> verify event fires', async () => {
    const db = new Database('test', join(tempDir, 'cdc.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    db.watch('users')
    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('insert')
    expect(received[0].table).toBe('users')
    expect(received[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })
    expect(received[0].oldRow).toBeUndefined()

    db.close()
  })

  it('captures UPDATE events with old and new row data', async () => {
    const db = new Database('test', join(tempDir, 'cdc-update.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await new Promise((resolve) => setTimeout(resolve, 120))
    received.length = 0

    db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('update')
    expect(received[0].row.age).toBe(31)
    expect(received[0].oldRow?.age).toBe(30)

    db.close()
  })

  it('captures DELETE events with old row data', async () => {
    const db = new Database('test', join(tempDir, 'cdc-delete.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise((resolve) => setTimeout(resolve, 120))
    received.length = 0

    db.execute("DELETE FROM users WHERE name = 'Alice'")
    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('delete')
    expect(received[0].oldRow?.name).toBe('Alice')

    db.close()
  })

  it('filters subscriptions to matching events only', async () => {
    const db = new Database('test', join(tempDir, 'cdc-filter.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.watch('users')

    const aliceOnly: ChangeEvent[] = []
    const allEvents: ChangeEvent[] = []

    db.on('users').filter({ name: 'Alice' }).subscribe((event) => aliceOnly.push(event))
    db.on('users').subscribe((event) => allEvents.push(event))

    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
    db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(allEvents).toHaveLength(3)
    expect(aliceOnly).toHaveLength(2)

    db.close()
  })

  it('unwatch stops capturing new changes', async () => {
    const db = new Database('test', join(tempDir, 'cdc-unwatch.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise((resolve) => setTimeout(resolve, 120))
    expect(received).toHaveLength(1)

    db.unwatch('users')
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received).toHaveLength(1)

    db.close()
  })

  it('throws ReadOnlyError when watching on a read-only database', () => {
    const dbPath = join(tempDir, 'ro-cdc.db')
    const setup = new Database('setup', dbPath)
    setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    setup.close()

    const db = new Database('test', dbPath, { readOnly: true })
    expect(() => db.watch('users')).toThrow(ReadOnlyError)
    db.close()
  })

  it('supports creating subscriptions before watching', async () => {
    const db = new Database('test', join(tempDir, 'cdc-sub-first.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.watch('users')
    db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.name).toBe('Alice')

    db.close()
  })

  it('respects custom CDC poll interval', async () => {
    const db = new Database('test', join(tempDir, 'cdc-interval.db'), { cdcPollInterval: 20 })
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe((event) => received.push(event))

    db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise((resolve) => setTimeout(resolve, 80))

    expect(received.length).toBeGreaterThanOrEqual(1)
    db.close()
  })

  it('cleans up CDC polling on close', async () => {
    const db = new Database('test', join(tempDir, 'cdc-close.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.watch('users')
    db.on('users').subscribe(() => {})

    db.close()

    await new Promise((resolve) => setTimeout(resolve, 120))
    expect(db.closed).toBe(true)
  })
})

describe('Hooks integration', () => {
  describe('Sirannon-level hooks via options', () => {
    it('fires onBeforeQuery and onAfterQuery hooks from options', () => {
      const beforeCalls: QueryHookContext[] = []
      const afterCalls: (QueryHookContext & { durationMs: number })[] = []

      const sir = new Sirannon({
        hooks: {
          onBeforeQuery: (ctx) => beforeCalls.push(ctx),
          onAfterQuery: (ctx) => afterCalls.push(ctx),
        },
      })

      const db = sir.open('main', join(tempDir, 'hooks.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      db.query('SELECT * FROM users')

      expect(beforeCalls.length).toBeGreaterThanOrEqual(3)
      expect(afterCalls.length).toBeGreaterThanOrEqual(3)
      expect(beforeCalls[0].databaseId).toBe('main')
      expect(afterCalls[0].durationMs).toBeGreaterThanOrEqual(0)

      sir.shutdown()
    })

    it('fires onDatabaseOpen and onDatabaseClose hooks', () => {
      const opened: string[] = []
      const closed: string[] = []

      const sir = new Sirannon({
        hooks: {
          onDatabaseOpen: (ctx) => opened.push(ctx.databaseId),
          onDatabaseClose: (ctx) => closed.push(ctx.databaseId),
        },
      })

      sir.open('db1', join(tempDir, 'db1.db'))
      sir.open('db2', join(tempDir, 'db2.db'))

      expect(opened).toEqual(['db1', 'db2'])
      expect(closed).toEqual([])

      sir.close('db1')
      expect(closed).toEqual(['db1'])

      sir.shutdown()
      expect(closed).toEqual(['db1', 'db2'])
    })

    it('fires onBeforeConnect hook and denies connection when hook throws', () => {
      const sir = new Sirannon({
        hooks: {
          onBeforeConnect: (ctx) => {
            if (ctx.databaseId === 'blocked') {
              throw new HookDeniedError('beforeConnect', 'access denied')
            }
          },
        },
      })

      expect(() => sir.open('blocked', join(tempDir, 'blocked.db'))).toThrow(HookDeniedError)
      expect(sir.has('blocked')).toBe(false)

      const db = sir.open('allowed', join(tempDir, 'allowed.db'))
      expect(db.id).toBe('allowed')

      sir.shutdown()
    })
  })

  describe('Sirannon-level hooks via registration methods', () => {
    it('registers beforeQuery hook that can deny operations', () => {
      const sir = new Sirannon()
      sir.onBeforeQuery((ctx) => {
        if (ctx.sql.includes('DROP')) {
          throw new HookDeniedError('beforeQuery', 'DROP statements are forbidden')
        }
      })

      const db = sir.open('main', join(tempDir, 'hooks-reg.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')

      expect(() => db.query('SELECT * FROM users')).not.toThrow()
      expect(() => db.execute('DROP TABLE users')).toThrow(HookDeniedError)

      const rows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
      expect(rows).toHaveLength(1)

      sir.shutdown()
    })
  })

  describe('Database-level hooks', () => {
    it('registers per-database hooks independently', () => {
      const sir = new Sirannon()
      const db1 = sir.open('db1', join(tempDir, 'db1-hooks.db'))
      const db2 = sir.open('db2', join(tempDir, 'db2-hooks.db'))

      db1.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)')
      db2.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)')

      const db1Queries: string[] = []
      const db2Queries: string[] = []

      db1.onBeforeQuery((ctx) => db1Queries.push(ctx.sql))
      db2.onBeforeQuery((ctx) => db2Queries.push(ctx.sql))

      db1.query('SELECT * FROM items')
      db2.query('SELECT * FROM items')
      db1.execute('INSERT INTO items (id) VALUES (1)')

      expect(db1Queries).toHaveLength(2)
      expect(db2Queries).toHaveLength(1)

      sir.shutdown()
    })

    it('global hooks fire before local hooks', () => {
      const order: string[] = []

      const sir = new Sirannon()
      sir.onBeforeQuery(() => order.push('global'))

      const db = sir.open('main', join(tempDir, 'hook-order.db'))
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      order.length = 0

      db.onBeforeQuery(() => order.push('local'))
      db.query('SELECT * FROM t')

      expect(order).toEqual(['global', 'local'])

      sir.shutdown()
    })
  })

  describe('standalone Database hooks (no Sirannon)', () => {
    it('local hooks work on standalone Database instances', () => {
      const db = new Database('solo', join(tempDir, 'solo-hooks.db'))
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      const queries: string[] = []
      db.onBeforeQuery((ctx) => queries.push(ctx.sql))
      db.query('SELECT * FROM t')

      expect(queries).toHaveLength(1)
      expect(queries[0]).toBe('SELECT * FROM t')

      db.close()
    })

    it('beforeQuery hook can deny operations on standalone Database', () => {
      const db = new Database('solo', join(tempDir, 'solo-deny.db'))
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

      db.onBeforeQuery(() => {
        throw new Error('denied')
      })

      expect(() => db.query('SELECT * FROM t')).toThrow('denied')
      db.close()
    })
  })
})

describe('Metrics integration', () => {
  it('tracks query metrics through MetricsConfig', () => {
    const queryMetrics: { sql: string; durationMs: number }[] = []

    const sir = new Sirannon({
      metrics: {
        onQueryComplete: (m) => queryMetrics.push({ sql: m.sql, durationMs: m.durationMs }),
      },
    })

    const db = sir.open('main', join(tempDir, 'metrics.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    db.query('SELECT * FROM users')

    expect(queryMetrics.length).toBeGreaterThanOrEqual(3)
    expect(queryMetrics.every((m) => m.durationMs >= 0)).toBe(true)

    sir.shutdown()
  })

  it('tracks connection open and close metrics', () => {
    const connectionEvents: { databaseId: string; event: 'open' | 'close' }[] = []

    const sir = new Sirannon({
      metrics: {
        onConnectionOpen: (m) => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
        onConnectionClose: (m) => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
      },
    })

    sir.open('db1', join(tempDir, 'metrics-conn1.db'))
    sir.open('db2', join(tempDir, 'metrics-conn2.db'))

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
    ])

    sir.close('db1')

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
      { databaseId: 'db1', event: 'close' },
    ])

    sir.shutdown()
  })

  it('tracks errors in query metrics', () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      metrics: {
        onQueryComplete: (m) => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = sir.open('main', join(tempDir, 'metrics-err.db'))

    try {
      db.query('SELECT * FROM nonexistent_table')
    } catch {
      // expected
    }

    expect(metrics.length).toBe(1)
    expect(metrics[0].error).toBe(true)

    sir.shutdown()
  })
})

describe('Lifecycle integration', () => {
  it('auto-opens databases via resolver on get()', () => {
    const dbPath = join(tempDir, 'auto-open.db')
    const setup = new Database('setup', dbPath)
    setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    setup.execute("INSERT INTO users (name) VALUES ('Alice')")
    setup.close()

    const sir = new Sirannon({
      lifecycle: {
        autoOpen: {
          resolver: (id) => {
            if (id === 'auto') return { path: dbPath }
            return undefined
          },
        },
      },
    })

    expect(sir.has('auto')).toBe(false)

    const db = sir.get('auto')
    expect(db).toBeDefined()
    expect(db!.id).toBe('auto')
    expect(sir.has('auto')).toBe(true)

    const rows = db!.query<{ name: string }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')

    expect(sir.get('unknown')).toBeUndefined()

    sir.shutdown()
  })

  it('closes idle databases after timeout', async () => {
    const sir = new Sirannon({
      lifecycle: {
        autoOpen: {
          resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
        },
        idleTimeout: 200,
      },
    })

    const db = sir.get('idle-test')
    expect(db).toBeDefined()
    expect(sir.has('idle-test')).toBe(true)

    await new Promise((resolve) => setTimeout(resolve, 400))

    expect(sir.has('idle-test')).toBe(false)

    sir.shutdown()
  })

  it('evicts LRU database when maxOpen is reached', () => {
    const sir = new Sirannon({
      lifecycle: {
        autoOpen: {
          resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
        },
        maxOpen: 2,
      },
    })

    sir.get('db1')
    sir.get('db2')

    expect(sir.databases().size).toBe(2)

    sir.get('db3')

    expect(sir.databases().size).toBe(2)
    expect(sir.has('db1')).toBe(false)
    expect(sir.has('db2')).toBe(true)
    expect(sir.has('db3')).toBe(true)

    sir.shutdown()
  })

  it('get() returns undefined after shutdown instead of throwing', () => {
    const sir = new Sirannon({
      lifecycle: {
        autoOpen: {
          resolver: () => ({ path: join(tempDir, 'post-shutdown.db') }),
        },
      },
    })

    sir.shutdown()

    expect(sir.get('anything')).toBeUndefined()
  })

  it('lifecycle hooks and metrics fire for auto-opened databases', () => {
    const opened: string[] = []
    const connectionEvents: string[] = []

    const sir = new Sirannon({
      hooks: {
        onDatabaseOpen: (ctx) => opened.push(ctx.databaseId),
      },
      metrics: {
        onConnectionOpen: (m) => connectionEvents.push(m.databaseId),
      },
      lifecycle: {
        autoOpen: {
          resolver: (id) => ({ path: join(tempDir, `${id}-lifecycle.db`) }),
        },
      },
    })

    sir.get('auto-hooks')

    expect(opened).toEqual(['auto-hooks'])
    expect(connectionEvents).toEqual(['auto-hooks'])

    sir.shutdown()
  })
})

describe('Backup integration via Database', () => {
  it('creates a one-shot backup via backup()', () => {
    const db = new Database('test', join(tempDir, 'backup-source.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const backupPath = join(tempDir, 'backup-copy.db')
    db.backup(backupPath)
    db.close()

    const verify = new Database('verify', backupPath)
    const rows = verify.query<{ name: string }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    verify.close()
  })

  it('throws when backing up a read-only database (no writer)', () => {
    const dbPath = join(tempDir, 'ro-backup.db')
    const setup = new Database('setup', dbPath)
    setup.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    setup.close()

    const db = new Database('test', dbPath, { readOnly: true })
    expect(() => db.backup(join(tempDir, 'fail.db'))).toThrow()
    db.close()
  })

  it('integrates backup through Sirannon', () => {
    const sir = new Sirannon()
    const db = sir.open('main', join(tempDir, 'sir-backup.db'))
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
    db.execute("INSERT INTO products (title) VALUES ('Widget')")

    const backupPath = join(tempDir, 'sir-backup-copy.db')
    db.backup(backupPath)

    const verify = new Database('verify', backupPath)
    const rows = verify.query<{ title: string }>('SELECT * FROM products')
    expect(rows).toHaveLength(1)
    expect(rows[0].title).toBe('Widget')
    verify.close()

    sir.shutdown()
  })
})

describe('Extension loading via Database', () => {
  it('throws ExtensionError for nonexistent extension path', () => {
    const db = new Database('test', join(tempDir, 'ext.db'))

    expect(() => db.loadExtension('/nonexistent/extension.so')).toThrow(SirannonError)
    expect(() => db.loadExtension('/nonexistent/extension.so')).toThrow('Failed to load extension')

    db.close()
  })
})

describe('End-to-end smoke test', () => {
  it('open -> create table -> watch -> subscribe -> insert -> verify event -> query -> backup -> close', async () => {
    const queryLog: string[] = []
    const connectionLog: string[] = []

    const sir = new Sirannon({
      hooks: {
        onBeforeQuery: (ctx) => queryLog.push(ctx.sql),
      },
      metrics: {
        onConnectionOpen: (m) => connectionLog.push(`open:${m.databaseId}`),
        onConnectionClose: (m) => connectionLog.push(`close:${m.databaseId}`),
      },
    })

    const db = sir.open('e2e', join(tempDir, 'e2e.db'))
    expect(connectionLog).toContain('open:e2e')

    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    db.watch('users')

    const events: ChangeEvent[] = []
    const sub = db.on('users').subscribe((event) => events.push(event))

    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].type).toBe('insert')
    expect(events[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })

    sub.unsubscribe()

    const rows = db.query<{ id: number; name: string; age: number }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')

    const backupPath = join(tempDir, 'e2e-backup.db')
    db.backup(backupPath)

    const verify = new Database('verify', backupPath)
    const backupRows = verify.query<{ name: string }>('SELECT * FROM users')
    expect(backupRows).toHaveLength(1)
    expect(backupRows[0].name).toBe('Alice')
    verify.close()

    expect(queryLog.length).toBeGreaterThan(0)

    sir.shutdown()
    expect(connectionLog).toContain('close:e2e')
  })
})

describe('Combined integration edge cases', () => {
  it('beforeQuery hook denial prevents metrics from recording a successful query', () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      hooks: {
        onBeforeQuery: (ctx) => {
          if (ctx.sql.includes('forbidden')) {
            throw new HookDeniedError('beforeQuery', 'forbidden query')
          }
        },
      },
      metrics: {
        onQueryComplete: (m) => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = sir.open('main', join(tempDir, 'hook-deny-metrics.db'))
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    expect(() => db.query('SELECT forbidden FROM t')).toThrow(HookDeniedError)

    const forbiddenMetrics = metrics.filter((m) => m.sql.includes('forbidden'))
    expect(forbiddenMetrics).toHaveLength(0)

    sir.shutdown()
  })

  it('afterQuery hook errors do not mask query errors', () => {
    const db = new Database('test', join(tempDir, 'after-mask.db'))
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    expect(() => db.query('INVALID SQL')).toThrow('syntax error')

    db.close()
  })

  it('afterQuery hook errors do not mask successful query results', () => {
    const db = new Database('test', join(tempDir, 'after-no-mask.db'))
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    db.execute("INSERT INTO t (val) VALUES ('hello')")

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    const rows = db.query<{ val: string }>('SELECT * FROM t')
    expect(rows).toHaveLength(1)
    expect(rows[0].val).toBe('hello')

    db.close()
  })

  it('multiple databases share global hooks but have independent local hooks', () => {
    const globalLog: string[] = []
    const localLog1: string[] = []
    const localLog2: string[] = []

    const sir = new Sirannon()
    sir.onBeforeQuery((ctx) => globalLog.push(`${ctx.databaseId}:${ctx.sql}`))

    const db1 = sir.open('db1', join(tempDir, 'multi-hook1.db'))
    const db2 = sir.open('db2', join(tempDir, 'multi-hook2.db'))

    db1.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    db2.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db1.onBeforeQuery((ctx) => localLog1.push(ctx.sql))
    db2.onBeforeQuery((ctx) => localLog2.push(ctx.sql))

    globalLog.length = 0

    db1.query('SELECT 1')
    db2.query('SELECT 2')

    expect(globalLog).toEqual(['db1:SELECT 1', 'db2:SELECT 2'])
    expect(localLog1).toEqual(['SELECT 1'])
    expect(localLog2).toEqual(['SELECT 2'])

    sir.shutdown()
  })
})

describe('CDC through Sirannon', () => {
  it('watch and subscribe work on databases opened through Sirannon', async () => {
    const sir = new Sirannon()
    const db = sir.open('main', join(tempDir, 'sir-cdc.db'))
    db.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)')
    db.watch('messages')

    const received: ChangeEvent[] = []
    db.on('messages').subscribe((event) => received.push(event))

    db.execute("INSERT INTO messages (body) VALUES ('hello world')")

    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.body).toBe('hello world')

    sir.shutdown()
  })
})

describe('Close listener cleanup', () => {
  it('lifecycle manager untracks databases when they close', () => {
    const sir = new Sirannon({
      lifecycle: {
        autoOpen: {
          resolver: (id) => ({ path: join(tempDir, `${id}-cleanup.db`) }),
        },
        idleTimeout: 60_000,
      },
    })

    const db = sir.get('tracked')
    expect(db).toBeDefined()
    expect(sir.has('tracked')).toBe(true)

    db!.close()

    expect(sir.has('tracked')).toBe(false)
  })
})
