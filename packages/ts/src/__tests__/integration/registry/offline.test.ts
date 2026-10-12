import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { connectionsHeldOn, freshDirectory, journalFormat, readTotals, seedOrders } from './sqlite-oracle.js'

const CREATE_ORDERS = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)'
const INSERT_ORDER = 'INSERT INTO orders (total) VALUES (?)'
const SELECT_TOTALS = 'SELECT total FROM orders ORDER BY id'

let dir: string
let sirannon: Sirannon

interface Gate {
  opened: Promise<void>
  open: () => void
}

function gate(): Gate {
  let open = (): void => {}
  const opened = new Promise<void>(resolve => {
    open = resolve
  })
  return { opened, open }
}

function replaceWithOrders(path: string, totals: readonly number[]): void {
  rmSync(path, { force: true })
  seedOrders(path, totals)
}

async function openOrders(name: string, options?: DatabaseOptions): Promise<string> {
  const path = join(dir, `${name}.db`)
  const db = await sirannon.open(name, path, options)
  await db.execute(CREATE_ORDERS)
  await db.execute(INSERT_ORDER, [10])
  return path
}

beforeEach(() => {
  dir = freshDirectory('offline')
  sirannon = new Sirannon({
    driver: betterSqlite3(),
    lifecycle: { autoOpen: { resolver: id => ({ path: join(dir, `${id}.db`) }) } },
  })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe('taking a database offline to replace its file', () => {
  it('closes every connection before the action runs, then serves the file that the action leaves behind', async () => {
    const path = await openOrders('orders')
    const before = sirannon.get('orders')
    const seen: { path: string; held: boolean; registered: boolean }[] = []

    const outcome = await sirannon.withDatabaseOffline('orders', async offlinePath => {
      seen.push({ path: offlinePath, held: connectionsHeldOn(offlinePath), registered: sirannon.has('orders') })
      replaceWithOrders(offlinePath, [99, 100])
      return 'replaced'
    })

    expect(outcome).toEqual({ ok: true, value: 'replaced' })
    expect(seen).toEqual([{ path, held: false, registered: false }])
    expect(before?.closed).toBe(true)
    const after = sirannon.get('orders')
    expect(after).not.toBe(before)
    expect(await after?.query(SELECT_TOTALS)).toEqual([{ total: 99 }, { total: 100 }])
    await after?.execute(INSERT_ORDER, [101])
    expect(readTotals(path)).toEqual([99, 100, 101])
  })

  it('opens the database again with the settings that it was first opened with', async () => {
    const rollbackPath = await openOrders('rollback', { walMode: false })
    const walPath = await openOrders('wal')

    for (const name of ['rollback', 'wal']) {
      await sirannon.withDatabaseOffline(name, async path => replaceWithOrders(path, [5]))
    }

    expect(journalFormat(rollbackPath)).toBe('rollback')
    expect(journalFormat(walPath)).toBe('wal')
    for (const name of ['rollback', 'wal']) {
      expect(await sirannon.get(name)?.query(SELECT_TOTALS), name).toEqual([{ total: 5 }])
    }
  })

  it('opens the database again after a failed action and returns the error that the action threw', async () => {
    const path = await openOrders('orders')

    const outcome = await sirannon.withDatabaseOffline('orders', async () => {
      throw new Error('the backup destination went away')
    })

    expect(outcome.ok).toBe(false)
    expect(outcome).toMatchObject({ failure: { message: 'the backup destination went away' } })
    expect(outcome.reopenFailure).toBeUndefined()
    expect(await sirannon.get('orders')?.query(SELECT_TOTALS)).toEqual([{ total: 10 }])
    expect(readTotals(path)).toEqual([10])
  })

  it('counts an action that returns undefined, null, zero, false, or an empty string as one that succeeded', async () => {
    await openOrders('orders')

    for (const value of [undefined, null, 0, false, '']) {
      const outcome = await sirannon.withDatabaseOffline('orders', async () => value)
      expect(outcome, String(value)).toEqual({ ok: true, value })
    }
    expect(await sirannon.get('orders')?.query(SELECT_TOTALS)).toEqual([{ total: 10 }])
  })

  it('reports a reopen that fails beside the result of the action, and leaves the identifier free', async () => {
    const path = await openOrders('orders')

    const outcome = await sirannon.withDatabaseOffline('orders', async offlinePath => {
      rmSync(offlinePath)
      mkdirSync(offlinePath)
      return 'moved aside'
    })

    expect(outcome).toMatchObject({ ok: true, value: 'moved aside', reopenFailure: { code: 'DATABASE_OPEN_FAILED' } })
    expect(sirannon.has('orders')).toBe(false)
    rmSync(path, { recursive: true })
    writeFileSync(path, '')
    const reopened = await sirannon.open('orders', path)
    await reopened.execute(CREATE_ORDERS)
    expect(readTotals(path)).toEqual([])
  })

  it('waits for a close that is already under way before the action runs', async () => {
    const path = await openOrders('orders')
    const closing = sirannon.get('orders')?.close()
    let heldDuringAction = true

    const outcome = await sirannon.withDatabaseOffline('orders', async offlinePath => {
      heldDuringAction = connectionsHeldOn(offlinePath)
    })
    await closing

    expect(heldDuringAction).toBe(false)
    expect(outcome.reopenFailure).toBeUndefined()
    expect(sirannon.databases().size).toBe(1)
    expect(await sirannon.get('orders')?.query(SELECT_TOTALS)).toEqual([{ total: 10 }])
    expect(readTotals(path)).toEqual([10])
  })

  it('answers no database for the identifier until the reopen finishes, so no second runtime opens over the file', async () => {
    const path = await openOrders('orders')
    const held = gate()
    const answersDuringAction: unknown[] = []
    let answerDuringReopen: Promise<unknown> = Promise.resolve('the reopen never connected')
    let reopening = false
    sirannon.onBeforeConnect(ctx => {
      if (!reopening || ctx.databaseId !== 'orders') return
      reopening = false
      answerDuringReopen = sirannon.resolve('orders')
    })
    let secondActionRan = false

    const first = sirannon.withDatabaseOffline('orders', async () => {
      await held.opened
      reopening = true
    })
    answersDuringAction.push(await sirannon.resolve('orders'), sirannon.get('orders'), sirannon.has('orders'))
    await expect(sirannon.open('orders', path)).rejects.toMatchObject({ code: 'DATABASE_ALREADY_EXISTS' })
    await expect(
      sirannon.withDatabaseOffline('orders', async () => {
        secondActionRan = true
      }),
    ).rejects.toMatchObject({ code: 'DATABASE_NOT_FOUND' })
    const heldDuringAction = connectionsHeldOn(path)
    held.open()
    const outcome = await first

    expect(answersDuringAction).toEqual([undefined, undefined, false])
    expect(heldDuringAction).toBe(false)
    expect(secondActionRan).toBe(false)
    await expect(answerDuringReopen).resolves.toBeUndefined()
    expect(outcome).toEqual({ ok: true, value: undefined })
    expect(sirannon.databases().size).toBe(1)
    expect(await sirannon.get('orders')?.query(SELECT_TOTALS)).toEqual([{ total: 10 }])
  })

  it('holds a shutdown until the replaced file is open again, then closes it', async () => {
    const path = await openOrders('orders')
    const held = gate()
    let shutdownFinished = false

    const offline = sirannon.withDatabaseOffline('orders', async offlinePath => {
      await held.opened
      replaceWithOrders(offlinePath, [42])
    })
    const shutdown = sirannon.shutdown().then(() => {
      shutdownFinished = true
    })
    await new Promise(resolve => setImmediate(resolve))
    const finishedBeforeRelease = shutdownFinished
    held.open()
    const outcome = await offline
    await shutdown

    expect(finishedBeforeRelease).toBe(false)
    expect(outcome).toEqual({ ok: true, value: undefined })
    expect(sirannon.has('orders')).toBe(false)
    expect(connectionsHeldOn(path)).toBe(false)
    expect(readTotals(path)).toEqual([42])
  })

  it('refuses an unknown identifier, a read-only database, and a shut-down registry without calling the action', async () => {
    const path = await openOrders('writable')
    await sirannon.close('writable')
    await sirannon.open('archive', path, { readOnly: true })
    let actionRan = false
    const action = async (): Promise<void> => {
      actionRan = true
    }

    await expect(sirannon.withDatabaseOffline('missing', action)).rejects.toMatchObject({
      code: 'DATABASE_NOT_FOUND',
    })
    await expect(sirannon.withDatabaseOffline('archive', action)).rejects.toMatchObject({ code: 'READ_ONLY' })
    expect(sirannon.has('archive')).toBe(true)
    await sirannon.shutdown()
    await expect(sirannon.withDatabaseOffline('archive', action)).rejects.toMatchObject({ code: 'SHUTDOWN' })

    expect(actionRan).toBe(false)
    expect(readTotals(path)).toEqual([10])
  })
})
