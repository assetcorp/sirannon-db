import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
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
        onDatabaseOpen: ctx => {
          opened.push(ctx.databaseId)
        },
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
