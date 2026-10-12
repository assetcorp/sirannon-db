import { existsSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { HookDeniedError, SirannonError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ConnectionHookContext, ConnectionMetrics, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  elapse,
  readFile,
  type ScratchDirectory,
  scratchDirectory,
  settle,
  watchUnhandledRejections,
  writeFile,
} from './harness.js'

const SEED_SCRIPT = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL);'
const INSERT_NOTE = 'INSERT INTO notes (body) VALUES (?)'

const driver = betterSqlite3()

let scratch: ScratchDirectory
const registries: Sirannon[] = []

function registry(options: Omit<SirannonOptions, 'driver'> = {}): Sirannon {
  const sirannon = new Sirannon({ driver, ...options })
  registries.push(sirannon)
  return sirannon
}

function seeded(name: string): string {
  const path = scratch.path(name)
  writeFile(path, SEED_SCRIPT)
  return path
}

beforeEach(() => {
  scratch = scratchDirectory('sirannon-connection-hooks-')
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  scratch.remove()
})

describe('a before-connect hook', () => {
  it('refuses to open a database when it throws or returns a promise, creates no file, and lets the same identifier open once it allows', async () => {
    const unhandled = watchUnhandledRejections()
    try {
      const denied = new HookDeniedError('beforeConnect', 'the tenant is suspended')
      const refusals: Array<[string, () => void | Promise<void>, (error: unknown) => void]> = [
        [
          'a HookDeniedError',
          () => {
            throw denied
          },
          error => expect(error).toBe(denied),
        ],
        [
          'an Error',
          () => {
            throw new Error('the tenant is suspended')
          },
          error => expect(error).toMatchObject({ code: 'DATABASE_OPEN_FAILED', message: /the tenant is suspended/ }),
        ],
        [
          'a string',
          () => {
            throw 'the tenant is suspended'
          },
          error => expect(error).toMatchObject({ code: 'DATABASE_OPEN_FAILED', message: /the tenant is suspended/ }),
        ],
        [
          'undefined',
          () => {
            throw undefined
          },
          error => expect(error).toMatchObject({ code: 'DATABASE_OPEN_FAILED' }),
        ],
        [
          'a promise that rejects',
          async () => {
            throw new Error('the tenant directory is offline')
          },
          error => expect(error).toMatchObject({ code: 'DATABASE_OPEN_FAILED', message: /returned a Promise/ }),
        ],
        [
          'a promise that resolves',
          async () => {},
          error => expect(error).toMatchObject({ code: 'DATABASE_OPEN_FAILED', message: /returned a Promise/ }),
        ],
      ]
      let gate: () => void | Promise<void> = () => {}
      const contexts: ConnectionHookContext[] = []
      const opened: string[] = []
      const connections: ConnectionMetrics[] = []
      const sirannon = registry({
        hooks: {
          onBeforeConnect: ctx => {
            contexts.push(ctx)
            return gate()
          },
          onDatabaseOpen: ctx => {
            opened.push(ctx.databaseId)
          },
        },
        metrics: { onConnectionOpen: metrics => connections.push(metrics) },
      })
      const path = scratch.path('tenant.db')

      for (const [label, refuse, check] of refusals) {
        gate = refuse
        const outcome = await settle(sirannon.open('tenant', path))
        expect(outcome.rejected, label).toBe(true)
        expect(outcome.value, label).toBeInstanceOf(SirannonError)
        check(outcome.value)
        expect(existsSync(path), label).toBe(false)
        expect(sirannon.has('tenant'), label).toBe(false)
      }
      expect(opened).toEqual([])
      expect(connections).toEqual([])

      gate = () => {}
      const db = await sirannon.open('tenant', path)
      await db.execute(SEED_SCRIPT)
      await db.execute(INSERT_NOTE, ['first note'])
      await elapse(60)

      expect(contexts).toEqual(Array.from({ length: refusals.length + 1 }, () => ({ databaseId: 'tenant', path })))
      expect(opened).toEqual(['tenant'])
      expect(readFile(path, 'SELECT id, body FROM notes')).toEqual([{ id: 1, body: 'first note' }])
      expect(unhandled.reasons).toEqual([])
    } finally {
      unhandled.stop()
    }
  })
})

describe('open and close hooks and the connection callbacks', () => {
  it('fire once per database in registration order, with its identifier, its path, and its reader count', async () => {
    const hookCalls: string[] = []
    const connections: ConnectionMetrics[] = []
    const log = (hook: string) => (ctx: ConnectionHookContext) => {
      hookCalls.push(`${hook} ${ctx.databaseId} ${ctx.path}`)
    }
    const sirannon = registry({
      hooks: { onDatabaseOpen: [log('open-first'), log('open-second')], onDatabaseClose: log('close-first') },
      metrics: {
        onConnectionOpen: metrics => connections.push(metrics),
        onConnectionClose: metrics => connections.push(metrics),
      },
    })
    sirannon.onDatabaseOpen(log('open-registered'))
    sirannon.onDatabaseClose(log('close-registered'))
    const [pathA, pathB, pathC] = [seeded('a.db'), seeded('b.db'), seeded('c.db')]

    await sirannon.open('tenant-a', pathA, { readPoolSize: 1 })
    const tenantB = await sirannon.open('tenant-b', pathB, { readPoolSize: 3 })
    await sirannon.open('tenant-c', pathC)
    const unreachable = await settle(sirannon.open('tenant-d', scratch.path('missing-directory/d.db')))
    await sirannon.close('tenant-a')
    await tenantB.close()
    await tenantB.close()
    await sirannon.shutdown()

    expect(unreachable.rejected).toBe(true)
    expect(unreachable.value).toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect(hookCalls).toEqual([
      `open-first tenant-a ${pathA}`,
      `open-second tenant-a ${pathA}`,
      `open-registered tenant-a ${pathA}`,
      `open-first tenant-b ${pathB}`,
      `open-second tenant-b ${pathB}`,
      `open-registered tenant-b ${pathB}`,
      `open-first tenant-c ${pathC}`,
      `open-second tenant-c ${pathC}`,
      `open-registered tenant-c ${pathC}`,
      `close-first tenant-a ${pathA}`,
      `close-registered tenant-a ${pathA}`,
      `close-first tenant-b ${pathB}`,
      `close-registered tenant-b ${pathB}`,
      `close-first tenant-c ${pathC}`,
      `close-registered tenant-c ${pathC}`,
    ])
    expect(connections).toEqual([
      { databaseId: 'tenant-a', path: pathA, readerCount: 1, event: 'open' },
      { databaseId: 'tenant-b', path: pathB, readerCount: 3, event: 'open' },
      { databaseId: 'tenant-c', path: pathC, readerCount: 4, event: 'open' },
      { databaseId: 'tenant-a', path: pathA, readerCount: 0, event: 'close' },
      { databaseId: 'tenant-b', path: pathB, readerCount: 0, event: 'close' },
      { databaseId: 'tenant-c', path: pathC, readerCount: 0, event: 'close' },
    ])
  })

  it('leave the database open, usable, and closable when a hook or a callback throws or returns a rejected promise', async () => {
    const unhandled = watchUnhandledRejections()
    try {
      const reached: string[] = []
      const sirannon = registry({
        hooks: {
          onDatabaseOpen: [
            () => {
              throw new Error('the inventory service is offline')
            },
            async () => {
              throw new Error('the async inventory service is offline')
            },
          ],
          onDatabaseClose: [
            () => {
              throw 'the close audit is offline'
            },
            () =>
              elapse(20).then(() => {
                throw new Error('the close audit timed out')
              }),
          ],
        },
        metrics: {
          onConnectionOpen: () => {
            throw new Error('the gauge is offline')
          },
          onConnectionClose: async () => {
            throw new Error('the async gauge is offline')
          },
        },
      })
      sirannon.onDatabaseOpen(ctx => {
        reached.push(`open ${ctx.databaseId}`)
      })
      sirannon.onDatabaseClose(ctx => {
        reached.push(`close ${ctx.databaseId}`)
      })
      const path = seeded('tenant.db')

      const db = await sirannon.open('tenant', path)
      const written = await db.execute(INSERT_NOTE, ['kept'])
      await sirannon.close('tenant')
      const reopened = await sirannon.open('tenant', path)
      const rows = await reopened.query('SELECT body FROM notes')
      await elapse(80)

      expect(written).toEqual({ changes: 1, lastInsertRowId: 1 })
      expect(rows).toEqual([{ body: 'kept' }])
      expect(readFile(path, 'SELECT id, body FROM notes')).toEqual([{ id: 1, body: 'kept' }])
      expect(reached).toEqual(['open tenant', 'close tenant', 'open tenant'])
      expect(unhandled.reasons).toEqual([])
    } finally {
      unhandled.stop()
    }
  })

  it('stop firing once their disposer runs, and a second call to the disposer changes nothing', async () => {
    const opened: string[] = []
    const closed: string[] = []
    const sirannon = registry()
    const disposeGate = sirannon.onBeforeConnect(() => {
      throw new HookDeniedError('beforeConnect', 'maintenance window')
    })
    const disposeOpen = sirannon.onDatabaseOpen(ctx => {
      opened.push(ctx.databaseId)
    })
    sirannon.onDatabaseOpen(ctx => {
      opened.push(`${ctx.databaseId} again`)
    })
    const disposeClose = sirannon.onDatabaseClose(ctx => {
      closed.push(ctx.databaseId)
    })
    const [firstPath, secondPath] = [scratch.path('first.db'), scratch.path('second.db')]

    const refused = await settle(sirannon.open('first', firstPath))
    disposeGate()
    disposeGate()
    await sirannon.open('first', firstPath)
    disposeOpen()
    disposeOpen()
    await sirannon.open('second', secondPath)
    await sirannon.close('first')
    disposeClose()
    disposeClose()
    await sirannon.close('second')

    expect(refused.value).toMatchObject({ code: 'HOOK_DENIED' })
    expect(opened).toEqual(['first', 'first again', 'second again'])
    expect(closed).toEqual(['first'])
  })
})
