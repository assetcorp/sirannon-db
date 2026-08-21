import { copyFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../sirannon.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string
let sirannon: Sirannon

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-offline-'))
  sirannon = new Sirannon({ driver: testDriver })
})

afterEach(async () => {
  await sirannon.shutdown().catch(() => {})
  rmSync(tempDir, { recursive: true, force: true })
})

async function openOrders(readPoolSize?: number) {
  const path = join(tempDir, 'orders.db')
  const db = await sirannon.open('orders', path, readPoolSize === undefined ? undefined : { readPoolSize })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
  await db.execute('INSERT INTO orders (total) VALUES (10)')
  return { db, path }
}

describe('taking one database offline while its file is replaced', () => {
  it('closes the database, hands the action its path, and opens it again under the same identifier', async () => {
    const { db, path } = await openOrders()
    const seen: { path: string; closed: boolean }[] = []

    await sirannon.withDatabaseOffline('orders', async offlinePath => {
      seen.push({ path: offlinePath, closed: db.closed })
    })

    expect(seen).toEqual([{ path, closed: true }])
    const reopened = sirannon.get('orders')
    expect(reopened).toBeDefined()
    expect(reopened).not.toBe(db)
    expect(await reopened?.query('SELECT total FROM orders')).toEqual([{ total: 10 }])
  })

  it('opens it again with the settings it was opened with the first time', async () => {
    await openOrders(2)

    await sirannon.withDatabaseOffline('orders', async () => {})

    expect(sirannon.get('orders')?.readerCount).toBe(2)
  })

  it('serves the file the action left behind, not the one it replaced', async () => {
    await openOrders()
    const spare = join(tempDir, 'spare.db')
    const other = await sirannon.open('spare', spare)
    await other.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
    await other.execute('INSERT INTO orders (total) VALUES (99)')
    await sirannon.close('spare')

    await sirannon.withDatabaseOffline('orders', async offlinePath => {
      copyFileSync(spare, offlinePath)
    })

    expect(await sirannon.get('orders')?.query('SELECT total FROM orders')).toEqual([{ total: 99 }])
  })

  it('opens the database again even where the action failed, and reports what the action threw', async () => {
    await openOrders()

    const outcome = await sirannon.withDatabaseOffline('orders', async () => {
      throw new Error('the destination went away')
    })

    expect(outcome.value).toBeUndefined()
    expect(outcome.failure).toMatchObject({ message: 'the destination went away' })
    expect(outcome.reopenFailure).toBeUndefined()
    expect(sirannon.has('orders')).toBe(true)
    expect(await sirannon.get('orders')?.query('SELECT total FROM orders')).toEqual([{ total: 10 }])
  })

  it('delays a shutdown until the file it was replacing is back in service', async () => {
    await openOrders()
    let release = (): void => {}
    const held = new Promise<void>(resolve => {
      release = resolve
    })
    let finished = false

    const offline = sirannon
      .withDatabaseOffline('orders', () => held)
      .then(() => {
        finished = true
      })
    const shutdown = sirannon.shutdown()
    release()
    await shutdown

    expect(finished).toBe(true)
    await offline
  })

  it('refuses an identifier with no open database under it', async () => {
    await expect(sirannon.withDatabaseOffline('missing', async () => {})).rejects.toMatchObject({
      code: 'DATABASE_NOT_FOUND',
    })
  })

  it('refuses a second call while the first still has the database offline', async () => {
    await openOrders()
    let release = (): void => {}
    const held = new Promise<void>(resolve => {
      release = resolve
    })

    const first = sirannon.withDatabaseOffline('orders', () => held)
    const second = sirannon.withDatabaseOffline('orders', async () => {})

    await expect(second).rejects.toMatchObject({ code: 'DATABASE_NOT_FOUND' })
    release()
    await first
  })

  it('answers nothing for an identifier while that database is offline, so no second runtime opens over its file', async () => {
    const path = join(tempDir, 'auto.db')
    const auto = new Sirannon({
      driver: testDriver,
      lifecycle: { autoOpen: { resolver: id => (id === 'orders' ? { path } : undefined) } },
    })
    const db = await auto.open('orders', path)
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    let release = (): void => {}
    const held = new Promise<void>(resolve => {
      release = resolve
    })

    const offline = auto.withDatabaseOffline('orders', () => held)
    const during = await auto.resolve('orders')

    expect(during).toBeUndefined()
    release()
    await offline
    expect(auto.has('orders')).toBe(true)
    await auto.shutdown()
  })

  it('refuses a database the registry opened read-only', async () => {
    const path = join(tempDir, 'readonly.db')
    const writable = await sirannon.open('seed', path)
    await writable.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await sirannon.close('seed')
    await sirannon.open('readonly', path, { readOnly: true })

    await expect(sirannon.withDatabaseOffline('readonly', async () => {})).rejects.toMatchObject({
      code: 'READ_ONLY',
    })
  })
})
