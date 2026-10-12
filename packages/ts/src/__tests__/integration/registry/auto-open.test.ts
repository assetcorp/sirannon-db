import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, LifecycleConfig } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { connectionsHeldOn, freshDirectory, journalFormat, readTotals, seedOrders } from './sqlite-oracle.js'

const SELECT_TOTALS = 'SELECT total FROM orders ORDER BY id'
const CONCURRENT_RESOLVES = 40
const BURST_TENANTS = 12
const BURST_MAX_OPEN = 3

let dir: string
const registries: Sirannon[] = []

function tenantFile(id: string): string {
  return join(dir, `${id}.db`)
}

function registry(lifecycle: Omit<LifecycleConfig, 'autoOpen'>, options?: (id: string) => DatabaseOptions): Sirannon {
  const sirannon = new Sirannon({
    driver: betterSqlite3(),
    lifecycle: { ...lifecycle, autoOpen: { resolver: id => ({ path: tenantFile(id), options: options?.(id) }) } },
  })
  registries.push(sirannon)
  return sirannon
}

async function resolved(sirannon: Sirannon, id: string): Promise<Database> {
  const db = await sirannon.resolve(id)
  if (db === undefined) throw new Error(`the resolver returned no database for '${id}'`)
  return db
}

function heldFiles(ids: readonly string[]): string[] {
  return ids.filter(id => connectionsHeldOn(tenantFile(id)))
}

beforeEach(() => {
  dir = freshDirectory('auto-open')
})

afterEach(async () => {
  vi.useRealTimers()
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe('opening a database on first use through the resolver', () => {
  it('opens an existing file on first use, serves the same database afterwards, and answers nothing for an identifier that the resolver refuses', async () => {
    seedOrders(tenantFile('orders'), [1, 2])
    const announced: string[] = []
    const refusing = new Sirannon({
      driver: betterSqlite3(),
      hooks: { onDatabaseOpen: ctx => void announced.push(`hook ${ctx.databaseId} ${ctx.path}`) },
      metrics: { onConnectionOpen: metrics => void announced.push(`metric ${metrics.databaseId} ${metrics.path}`) },
      lifecycle: { autoOpen: { resolver: id => (id === 'orders' ? { path: tenantFile(id) } : undefined) } },
    })
    const noResolver = new Sirannon({ driver: betterSqlite3() })
    registries.push(refusing, noResolver)

    expect(refusing.has('orders')).toBe(false)
    const first = await resolved(refusing, 'orders')

    expect(await first.query(SELECT_TOTALS)).toEqual([{ total: 1 }, { total: 2 }])
    expect(await refusing.resolve('orders')).toBe(first)
    expect(refusing.has('orders')).toBe(true)
    expect(announced).toEqual([`hook orders ${tenantFile('orders')}`, `metric orders ${tenantFile('orders')}`])
    expect(await refusing.resolve('invoices')).toBeUndefined()
    expect(existsSync(tenantFile('invoices'))).toBe(false)
    expect(await noResolver.resolve('orders')).toBeUndefined()
    expect(noResolver.has('orders')).toBe(false)
  })

  it('shares one open between concurrent resolves of one identifier, whether the open succeeds or fails', async () => {
    const sirannon = registry({})
    let connects = 0
    sirannon.onBeforeConnect(() => {
      connects++
    })
    mkdirSync(tenantFile('blocked'))

    const opened = await Promise.all(Array.from({ length: CONCURRENT_RESOLVES }, () => sirannon.resolve('orders')))
    const failures = await Promise.all(
      Array.from({ length: CONCURRENT_RESOLVES }, () =>
        sirannon.resolve('blocked').then(
          () => null,
          err => err,
        ),
      ),
    )

    expect(new Set(opened).size).toBe(1)
    expect(opened[0]?.closed).toBe(false)
    expect(new Set(failures).size).toBe(1)
    expect(failures[0]).toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect(connects).toBe(2)
    expect(sirannon.databases().size).toBe(1)

    rmSync(tenantFile('blocked'), { recursive: true })
    const recovered = await resolved(sirannon, 'blocked')
    expect(recovered.closed).toBe(false)
    expect(connects).toBe(3)
  })

  it('opens each database with the options that the resolver returns for it', async () => {
    seedOrders(tenantFile('archive'), [3])
    const sirannon = registry({}, id => {
      if (id === 'legacy') return { walMode: false }
      if (id === 'archive') return { readOnly: true }
      return {}
    })

    const legacy = await resolved(sirannon, 'legacy')
    const current = await resolved(sirannon, 'current')
    const archive = await resolved(sirannon, 'archive')
    for (const db of [legacy, current]) await db.execute('CREATE TABLE notes (body TEXT)')

    expect(journalFormat(tenantFile('legacy'))).toBe('rollback')
    expect(journalFormat(tenantFile('current'))).toBe('wal')
    await expect(archive.execute('INSERT INTO orders (total) VALUES (4)')).rejects.toMatchObject({ code: 'READ_ONLY' })
    expect(await archive.query(SELECT_TOTALS)).toEqual([{ total: 3 }])
    expect(readTotals(tenantFile('archive'))).toEqual([3])
  })
})

describe('the limit on open databases', () => {
  it('closes the least recently used database to make room, counting a get or a resolve as use even within one clock tick', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const tenants = ['acme', 'globex', 'initech', 'umbrella', 'hooli']
    const sirannon = registry({ maxOpen: 3 })
    for (const id of ['acme', 'globex', 'initech']) await resolved(sirannon, id)

    sirannon.get('acme')
    await resolved(sirannon, 'umbrella')
    const afterUmbrella = { open: [...sirannon.databases().keys()].sort(), held: heldFiles(tenants).sort() }
    await resolved(sirannon, 'acme')
    await resolved(sirannon, 'hooli')

    expect(afterUmbrella).toEqual({ open: ['acme', 'initech', 'umbrella'], held: ['acme', 'initech', 'umbrella'] })
    expect([...sirannon.databases().keys()].sort()).toEqual(['acme', 'hooli', 'umbrella'])
    expect(heldFiles(tenants).sort()).toEqual(['acme', 'hooli', 'umbrella'])
  })

  it('keeps no more databases open than the limit while many tenants arrive at once', async () => {
    const sirannon = registry({ maxOpen: BURST_MAX_OPEN })
    const tenants = Array.from({ length: BURST_TENANTS }, (_, at) => `tenant-${at}`)
    let peakHeld = 0
    sirannon.onDatabaseOpen(() => {
      peakHeld = Math.max(peakHeld, heldFiles(tenants).length)
    })

    const outcomes = await Promise.allSettled(tenants.map(id => sirannon.resolve(id)))

    expect(outcomes.filter(outcome => outcome.status === 'rejected')).toEqual([])
    expect(peakHeld).toBeLessThanOrEqual(BURST_MAX_OPEN)
    expect(heldFiles(tenants)).toHaveLength(BURST_MAX_OPEN)
    expect(sirannon.databases().size).toBe(BURST_MAX_OPEN)
  })

  it('counts databases opened by hand toward the limit', async () => {
    const tenants = ['manual', 'acme', 'globex']
    const sirannon = registry({ maxOpen: 2 })
    await sirannon.open('manual', tenantFile('manual'))

    for (const id of ['acme', 'globex']) await resolved(sirannon, id)

    expect(sirannon.databases().size).toBe(2)
    expect(heldFiles(tenants)).toHaveLength(2)
    expect(sirannon.has('globex')).toBe(true)
  })

  it('counts databases taken offline toward the limit, and refuses with MAX_DATABASES while they hold every place', async () => {
    const tenants = ['acme', 'globex', 'initech', 'hooli']
    const sirannon = registry({ maxOpen: 2 })
    for (const id of ['acme', 'globex']) await resolved(sirannon, id)
    let release = (): void => {}
    const held = new Promise<void>(resolve => {
      release = resolve
    })

    const acmeOffline = sirannon.withDatabaseOffline('acme', () => held)
    await resolved(sirannon, 'initech')
    const initechOffline = sirannon.withDatabaseOffline('initech', () => held)
    const refusal = await sirannon.resolve('hooli').then(
      () => null,
      err => err,
    )
    release()
    const outcomes = await Promise.all([acmeOffline, initechOffline])

    expect(refusal).toMatchObject({ code: 'MAX_DATABASES' })
    expect(existsSync(tenantFile('hooli'))).toBe(false)
    expect(outcomes.map(outcome => outcome.reopenFailure)).toEqual([undefined, undefined])
    expect([...sirannon.databases().keys()].sort()).toEqual(['acme', 'initech'])
    expect(heldFiles(tenants).sort()).toEqual(['acme', 'initech'])
  })

  it('sets no limit when maxOpen is zero, negative, not a number, or infinite', async () => {
    const tenants = ['acme', 'globex', 'initech', 'umbrella', 'hooli']
    for (const maxOpen of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      const sirannon = registry({ maxOpen })
      for (const id of tenants) await resolved(sirannon, id)
      expect(sirannon.databases().size, String(maxOpen)).toBe(tenants.length)
      expect(heldFiles(tenants), String(maxOpen)).toHaveLength(tenants.length)
      await sirannon.shutdown()
    }
  })
})
