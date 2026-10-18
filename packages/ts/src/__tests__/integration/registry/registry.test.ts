import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ConnectionMetrics } from '../../../core/metrics-types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { FAILING_CLOSE_FILE_MARKER, failingCloseDriver } from '../../helpers/fault-drivers/failing-close-driver.js'
import { connectionsHeldOn, freshDirectory, readTotals } from './sqlite-oracle.js'

const CREATE_ORDERS = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)'
const INSERT_ORDER = 'INSERT INTO orders (total) VALUES (?)'
const CONCURRENT_TENANTS = 24

let dir: string
const registries: Sirannon[] = []

function registry(options?: Omit<SirannonOptions, 'driver'>, driver = betterSqlite3()): Sirannon {
  const sirannon = new Sirannon({ driver, ...options })
  registries.push(sirannon)
  return sirannon
}

beforeEach(() => {
  dir = freshDirectory('core')
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe('a registry of databases', () => {
  it('keeps each database on its own file and reports exactly the open ones through get, has, and databases', async () => {
    const sirannon = registry()
    const ordersPath = join(dir, 'orders.db')
    const invoicesPath = join(dir, 'invoices.db')
    const orders = await sirannon.open('orders', ordersPath)
    const invoices = await sirannon.open('invoices', invoicesPath)
    for (const db of [orders, invoices]) await db.execute(CREATE_ORDERS)
    await orders.execute(INSERT_ORDER, [10])
    await invoices.execute(INSERT_ORDER, [20])
    await invoices.execute(INSERT_ORDER, [30])

    expect(readTotals(ordersPath)).toEqual([10])
    expect(readTotals(invoicesPath)).toEqual([20, 30])
    expect(sirannon.get('orders')).toBe(orders)
    expect(sirannon.get('invoices')).toBe(invoices)
    for (const unknown of ['ORDERS', 'orders ', '', 'missing']) {
      expect(sirannon.get(unknown), unknown).toBeUndefined()
      expect(sirannon.has(unknown), unknown).toBe(false)
    }

    const listed = sirannon.databases()
    listed.delete('orders')
    listed.clear()
    expect([...sirannon.databases().keys()].sort()).toEqual(['invoices', 'orders'])
    expect(sirannon.has('orders')).toBe(true)
  })

  it('refuses a second open under an identifier that is open or still opening, and creates no file for it', async () => {
    const sirannon = registry()
    const ordersPath = join(dir, 'orders.db')
    const racingPath = join(dir, 'racing.db')
    const laterPath = join(dir, 'later.db')

    const first = sirannon.open('orders', ordersPath)
    await expect(sirannon.open('orders', racingPath)).rejects.toMatchObject({ code: 'DATABASE_ALREADY_EXISTS' })
    const orders = await first
    await expect(sirannon.open('orders', laterPath)).rejects.toMatchObject({ code: 'DATABASE_ALREADY_EXISTS' })

    expect(existsSync(racingPath)).toBe(false)
    expect(existsSync(laterPath)).toBe(false)
    expect(sirannon.get('orders')).toBe(orders)
    await orders.execute(CREATE_ORDERS)
    expect(readTotals(ordersPath)).toEqual([])
  })

  it('names the identifier and the path when a file cannot open, and leaves the identifier free', async () => {
    const sirannon = registry()
    const folder = join(dir, 'folder')
    mkdirSync(folder)
    const notSqlite = join(dir, 'notes.db')
    const notSqliteBytes = 'these bytes are a shopping list, not a database\n'.repeat(200)
    writeFileSync(notSqlite, notSqliteBytes)

    for (const path of [join(dir, 'missing', 'orders.db'), folder, notSqlite]) {
      const opening = sirannon.open('orders', path)
      await expect(opening, path).rejects.toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
      await expect(opening, path).rejects.toThrow(`'orders'`)
      await expect(opening, path).rejects.toThrow(path)
      expect(sirannon.has('orders'), path).toBe(false)
    }

    expect(existsSync(join(dir, 'missing'))).toBe(false)
    expect(readFileSync(notSqlite, 'utf8')).toBe(notSqliteBytes)
    const recovered = await sirannon.open('orders', join(dir, 'orders.db'))
    await recovered.execute(CREATE_ORDERS)
    expect(readTotals(join(dir, 'orders.db'))).toEqual([])
  })

  it('calls the connect, open, and close hooks and the connection metrics once for each database, and a connect hook that throws stops the open before any file exists', async () => {
    const events: string[] = []
    const recordConnection = (metrics: ConnectionMetrics): void => {
      events.push(`${metrics.event} metric ${metrics.databaseId} ${metrics.path}`)
    }
    const sirannon = registry({
      hooks: { onDatabaseOpen: ctx => void events.push(`open ${ctx.databaseId} ${ctx.path}`) },
      metrics: { onConnectionOpen: recordConnection, onConnectionClose: recordConnection },
    })
    sirannon.onBeforeConnect(ctx => {
      events.push(`connect ${ctx.databaseId} ${ctx.path}`)
      if (ctx.databaseId === 'suspended') throw new Error('this tenant is suspended')
    })
    const stopRecordingCloses = sirannon.onDatabaseClose(ctx => void events.push(`close ${ctx.databaseId} ${ctx.path}`))
    sirannon.onDatabaseOpen(() => {
      throw new Error('an open hook that fails')
    })
    sirannon.onDatabaseClose(() => {
      throw new Error('a close hook that fails')
    })
    const ordersPath = join(dir, 'orders.db')
    const invoicesPath = join(dir, 'invoices.db')
    const suspendedPath = join(dir, 'suspended.db')

    await sirannon.open('orders', ordersPath)
    await sirannon.close('orders')
    const invoices = await sirannon.open('invoices', invoicesPath)
    await invoices.close()
    await expect(sirannon.open('suspended', suspendedPath)).rejects.toThrow('this tenant is suspended')

    expect(events).toEqual([
      `connect orders ${ordersPath}`,
      `open orders ${ordersPath}`,
      `open metric orders ${ordersPath}`,
      `close orders ${ordersPath}`,
      `close metric orders ${ordersPath}`,
      `connect invoices ${invoicesPath}`,
      `open invoices ${invoicesPath}`,
      `open metric invoices ${invoicesPath}`,
      `close invoices ${invoicesPath}`,
      `close metric invoices ${invoicesPath}`,
      `connect suspended ${suspendedPath}`,
    ])
    expect(existsSync(suspendedPath)).toBe(false)
    expect(sirannon.has('suspended')).toBe(false)

    stopRecordingCloses()
    events.length = 0
    await sirannon.open('orders', ordersPath)
    await sirannon.close('orders')
    expect(events).toEqual([
      `connect orders ${ordersPath}`,
      `open orders ${ordersPath}`,
      `open metric orders ${ordersPath}`,
      `close metric orders ${ordersPath}`,
    ])
  })

  it('releases the file and frees the identifier whether the database closes through the registry or on its own', async () => {
    const sirannon = registry()
    const path = join(dir, 'orders.db')
    const db = await sirannon.open('orders', path)
    await db.execute(CREATE_ORDERS)
    await db.execute(INSERT_ORDER, [10])

    await db.close()
    expect(connectionsHeldOn(path)).toBe(false)
    expect(sirannon.has('orders')).toBe(false)
    expect(sirannon.get('orders')).toBeUndefined()
    expect(sirannon.databases().size).toBe(0)
    await expect(sirannon.close('orders')).rejects.toMatchObject({ code: 'DATABASE_NOT_FOUND' })
    await expect(db.query('SELECT total FROM orders')).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })

    const reopened = await sirannon.open('orders', path)
    expect(await reopened.query('SELECT total FROM orders')).toEqual([{ total: 10 }])
    await reopened.execute(INSERT_ORDER, [20])
    await sirannon.close('orders')

    expect(reopened.closed).toBe(true)
    expect(connectionsHeldOn(path)).toBe(false)
    expect(sirannon.has('orders')).toBe(false)
    expect(readTotals(path)).toEqual([10, 20])
  })

  it('returns from a close that starts while another close is under way only once the file is released', async () => {
    const sirannon = registry()
    const path = join(dir, 'orders.db')
    const db = await sirannon.open('orders', path)
    await db.execute(CREATE_ORDERS)

    const firstClose = db.close()
    await sirannon.close('orders')
    const releasedAfterSecondClose = !connectionsHeldOn(path)
    await db.close()
    const releasedAfterThirdClose = !connectionsHeldOn(path)
    await firstClose

    expect([releasedAfterSecondClose, releasedAfterThirdClose]).toEqual([true, true])
    expect(sirannon.has('orders')).toBe(false)
  })

  it('opens many databases at once, each on its own file, with no rows crossing between them', async () => {
    const sirannon = registry()
    const tenants = Array.from({ length: CONCURRENT_TENANTS }, (_, at) => `tenant-${at}`)

    await Promise.all(
      tenants.map(async (tenant, at) => {
        const db = await sirannon.open(tenant, join(dir, `${tenant}.db`))
        await db.execute(CREATE_ORDERS)
        for (let order = 0; order <= at; order++) await db.execute(INSERT_ORDER, [at * 1000 + order])
      }),
    )

    expect(sirannon.databases().size).toBe(CONCURRENT_TENANTS)
    tenants.forEach((tenant, at) => {
      const expected = Array.from({ length: at + 1 }, (_, order) => at * 1000 + order)
      expect(readTotals(join(dir, `${tenant}.db`)), tenant).toEqual(expected)
    })
  })
})

describe('shutting a registry down', () => {
  it('closes every database and releases every file, then refuses opens and closes while get, has, and resolve report nothing', async () => {
    const sirannon = registry({
      lifecycle: { autoOpen: { resolver: id => ({ path: join(dir, `${id}.db`) }) } },
    })
    const manual = await sirannon.open('manual', join(dir, 'manual.db'))
    const resolved = await sirannon.resolve('resolved')
    if (resolved === undefined) throw new Error('the resolver returned no database for a valid identifier')
    for (const db of [manual, resolved]) await db.execute(CREATE_ORDERS)
    await manual.execute(INSERT_ORDER, [7])

    await sirannon.shutdown()
    await sirannon.shutdown()

    for (const name of ['manual', 'resolved']) {
      expect(connectionsHeldOn(join(dir, `${name}.db`)), name).toBe(false)
      expect(sirannon.get(name), name).toBeUndefined()
      expect(sirannon.has(name), name).toBe(false)
      expect(await sirannon.resolve(name), name).toBeUndefined()
    }
    expect([manual.closed, resolved.closed]).toEqual([true, true])
    expect(sirannon.databases().size).toBe(0)
    expect(readTotals(join(dir, 'manual.db'))).toEqual([7])
    await expect(sirannon.open('late', join(dir, 'late.db'))).rejects.toMatchObject({ code: 'SHUTDOWN' })
    await expect(sirannon.close('manual')).rejects.toMatchObject({ code: 'SHUTDOWN' })
    expect(await sirannon.resolve('never-opened')).toBeUndefined()
    expect(existsSync(join(dir, 'late.db'))).toBe(false)
    expect(existsSync(join(dir, 'never-opened.db'))).toBe(false)
  })

  it('reports SHUTDOWN_ERROR when one database fails to close, and still releases every other file', async () => {
    const sirannon = registry(undefined, failingCloseDriver())
    const names = ['first', `second-${FAILING_CLOSE_FILE_MARKER}`, 'third']
    for (const name of names) {
      const db = await sirannon.open(name, join(dir, `${name}.db`))
      await db.execute(CREATE_ORDERS)
    }

    await expect(sirannon.shutdown()).rejects.toMatchObject({ code: 'SHUTDOWN_ERROR' })

    for (const name of names) expect(connectionsHeldOn(join(dir, `${name}.db`)), name).toBe(false)
    expect(sirannon.databases().size).toBe(0)
    await expect(sirannon.shutdown()).resolves.toBeUndefined()
  })

  it('fails an open or a resolve that is still under way when the shutdown starts, and leaves no connection on its file', async () => {
    const sirannon = registry({
      lifecycle: { autoOpen: { resolver: id => ({ path: join(dir, `${id}.db`) }) } },
    })

    const opening = sirannon.open('opening', join(dir, 'opening.db'))
    const resolving = sirannon.resolve('resolving')
    const shutdown = sirannon.shutdown()

    await expect(opening).rejects.toMatchObject({ code: 'SHUTDOWN' })
    await expect(resolving).rejects.toMatchObject({ code: 'SHUTDOWN' })
    await shutdown
    for (const name of ['opening', 'resolving']) {
      expect(connectionsHeldOn(join(dir, `${name}.db`)), name).toBe(false)
      expect(sirannon.has(name), name).toBe(false)
    }
  })
})
