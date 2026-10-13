// @vitest-environment jsdom

import { createElement, type ReactElement, StrictMode, useEffect, useState } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { renderToString } from 'react-dom/server'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { Params } from '../../../core/types.js'
import { type LiveDatabase, type LiveQueryState, useCommand, useLiveQuery } from '../../../react/index.js'
import { MARKED_READ_COMMENT, markedReadDriver } from '../../helpers/fault-drivers/marked-read-driver.js'
import {
  createWorkspace,
  type LiveStatementLog,
  logLiveStatements,
  pause,
  QUIET_PERIOD_MS,
  type Workspace,
  waitFor,
} from './harness.js'

const ORDERS = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, reference TEXT NOT NULL, status TEXT NOT NULL)'
const BY_STATUS = 'SELECT id, reference FROM orders WHERE status = ? ORDER BY id'
const INSERT = 'INSERT INTO orders (id, reference, status) VALUES (?, ?, ?)'

interface Order {
  id: number
  reference: string
}

interface Mounted {
  container: HTMLElement
  root: Root
  unmounted: boolean
}

let workspace: Workspace
let log: LiveStatementLog
const mounted: Mounted[] = []

beforeEach(() => {
  workspace = createWorkspace('react-hooks')
  log = logLiveStatements()
})

afterEach(async () => {
  for (const entry of mounted.splice(0)) if (!entry.unmounted) entry.root.unmount()
  document.body.innerHTML = ''
  await workspace.dispose()
})

async function openOrders(name: string, rows: [number, string, string][] = []): Promise<Database> {
  const db = await workspace.open(name, undefined, { metrics: log.metrics })
  await db.execute(ORDERS)
  if (rows.length > 0) await db.executeBatch(INSERT, rows)
  return db
}

function mount(element: ReactElement): Mounted {
  const container = document.createElement('div')
  document.body.appendChild(container)
  const entry: Mounted = { container, root: createRoot(container), unmounted: false }
  entry.root.render(element)
  mounted.push(entry)
  return entry
}

function unmount(entry: Mounted): void {
  entry.root.unmount()
  entry.unmounted = true
}

function describeState(state: LiveQueryState<Order>): string {
  if (state.status === 'ready') return state.rows.map(row => `${row.id}:${row.reference}`).join(',') || 'none'
  if (state.status === 'error') return `error ${(state.error as { code?: string }).code ?? state.error.message}`
  return state.status
}

async function oracleText(db: Database, sql: string, params: Params): Promise<string> {
  const rows = await db.query<Order>(sql, params)
  return rows.map(row => `${row.id}:${row.reference}`).join(',') || 'none'
}

async function shows(entry: Mounted, text: string): Promise<void> {
  await waitFor(
    () => entry.container.textContent === text,
    `the page to show '${text}', showing '${entry.container.textContent}'`,
  )
}

async function openLiveQueries(db: Database, id: number, status: string): Promise<number> {
  const before = log.returned().length
  await db.execute(INSERT, [id, `probe-${id}`, status])
  await pause(QUIET_PERIOD_MS)
  return log.returned().length - before
}

interface OrderListProps {
  database: LiveDatabase
  status: string
  label?: string
  enabled?: boolean
}

function OrderList({ database, status, label = '', enabled }: OrderListProps): ReactElement {
  const state = useLiveQuery<Order>(database, BY_STATUS, [status], {
    enabled,
    rereadJitterMs: 0,
    maxTransactionChanges: 1,
  })
  return createElement('p', null, `${label}${describeState(state)}`)
}

describe('useLiveQuery over a local database', () => {
  it('renders the rows of a statement in every reader and re-renders after each write, as db.query reads them', async () => {
    const db = await openOrders('renders', [
      [1, 'A-1', 'open'],
      [2, 'A-2', 'paid'],
    ])
    const page = mount(
      createElement(
        'div',
        null,
        createElement(OrderList, { database: db, status: 'open', label: 'left ' }),
        createElement(OrderList, { database: db, status: 'open', label: ' right ' }),
      ),
    )
    const expected = async () => {
      const text = await oracleText(db, BY_STATUS, ['open'])
      return `left ${text} right ${text}`
    }

    await shows(page, await expected())
    await db.execute(INSERT, [3, 'A-3', 'open'])
    await shows(page, await expected())
    await db.execute("UPDATE orders SET status = 'open' WHERE id = 2")
    await shows(page, await expected())
    const readsBefore = log.returned().filter(count => count >= 3).length
    await db.executeBatch(INSERT, [
      [4, 'A-4', 'open'],
      [5, 'A-5', 'open'],
    ])
    await shows(page, await expected())

    expect(log.returned().filter(count => count >= 3).length - readsBefore).toBe(2)
  })

  it('keeps one live query per argument set across re-renders and StrictMode, BigInt and BLOB arguments included', async () => {
    const db = await workspace.open('one-query', undefined, { metrics: log.metrics })
    await db.execute('CREATE TABLE accounts (id INTEGER PRIMARY KEY, number INTEGER NOT NULL, token BLOB NOT NULL)')
    await db.execute('INSERT INTO accounts VALUES (1, ?, ?)', [9007199254740993n, new Uint8Array([1, 2, 3])])
    await db.execute('INSERT INTO accounts VALUES (2, ?, ?)', [5, new Uint8Array([9])])
    const sql = 'SELECT id FROM accounts WHERE number = ? AND token = ? ORDER BY id'
    let rerender = (): void => {}
    let switchAccount = (): void => {}

    function Account(): ReactElement {
      const [, setTick] = useState(0)
      const [second, setSecond] = useState(false)
      useEffect(() => {
        rerender = () => setTick(tick => tick + 1)
        switchAccount = () => setSecond(true)
      }, [])
      const args = second ? [5, new Uint8Array([9])] : [9007199254740993n, new Uint8Array([1, 2, 3])]
      const state = useLiveQuery<{ id: number }>(db, sql, args, { rereadJitterMs: 0 })
      return createElement('p', null, state.status === 'ready' ? state.rows.map(row => row.id).join(',') : state.status)
    }

    const statementsAfter = async (write: () => Promise<unknown>, text: string): Promise<number> => {
      const before = log.returned().length
      await write()
      await shows(page, text)
      await pause(QUIET_PERIOD_MS)
      return log.returned().length - before
    }

    const page = mount(createElement(StrictMode, null, createElement(Account)))
    await shows(page, '1')
    for (let round = 0; round < 3; round++) {
      rerender()
      await pause(10)
    }
    await pause(QUIET_PERIOD_MS)
    const firstReads = log.returned().length

    const checksOfFirst = await statementsAfter(
      () => db.execute('INSERT INTO accounts VALUES (3, ?, ?)', [9007199254740993n, new Uint8Array([1, 2, 3])]),
      '1,3',
    )
    switchAccount()
    await shows(page, '2')
    await pause(QUIET_PERIOD_MS)
    const checksOfSecond = await statementsAfter(
      () => db.execute('INSERT INTO accounts VALUES (4, ?, ?)', [5, new Uint8Array([9])]),
      '2,4',
    )

    expect(firstReads).toBeLessThanOrEqual(2)
    expect(checksOfFirst).toBe(1)
    expect(checksOfSecond).toBe(1)
  })

  it('closes the query on unmount, including one whose first read finishes after the unmount', async () => {
    const faults = markedReadDriver()
    const db = await workspace.open('unmount', undefined, { metrics: log.metrics, driver: faults.driver })
    await db.execute(ORDERS)
    await db.execute(INSERT, [1, 'A-1', 'open'])
    const marked = `SELECT id, reference FROM orders WHERE status = ? ORDER BY ${MARKED_READ_COMMENT} id`

    function Marked(): ReactElement {
      const state = useLiveQuery<Order>(db, marked, ['open'], { rereadJitterMs: 0 })
      return createElement('p', null, describeState(state))
    }

    const settled = mount(createElement(Marked))
    await shows(settled, '1:A-1')
    unmount(settled)
    expect(await openLiveQueries(db, 10, 'open')).toBe(0)

    let releaseRead = (): void => {}
    const readHeld = new Promise<void>(resolve => {
      releaseRead = resolve
    })
    let readStarted = false
    faults.afterNextMarkedRead(async () => {
      readStarted = true
      await readHeld
    })
    const early = mount(createElement(Marked))
    await waitFor(() => readStarted, 'the first read to start')
    expect(early.container.textContent).toBe('pending')
    unmount(early)
    releaseRead()
    await pause(QUIET_PERIOD_MS)

    expect(await openLiveQueries(db, 11, 'open')).toBe(0)
  })

  it('stays closed while disabled, opens once enabled, and closes when disabled again', async () => {
    const db = await openOrders('enabled', [[1, 'A-1', 'open']])
    let setEnabled = (_enabled: boolean): void => {}

    function Toggle(): ReactElement {
      const [enabled, update] = useState(false)
      useEffect(() => {
        setEnabled = update
      }, [])
      return createElement(OrderList, { database: db, status: 'open', enabled })
    }

    const page = mount(createElement(Toggle))
    await shows(page, 'pending')
    expect(await openLiveQueries(db, 2, 'open')).toBe(0)
    expect(log.returned()).toEqual([])

    setEnabled(true)
    await shows(page, await oracleText(db, BY_STATUS, ['open']))
    expect(await openLiveQueries(db, 3, 'open')).toBe(1)

    setEnabled(false)
    await shows(page, 'pending')
    expect(await openLiveQueries(db, 4, 'open')).toBe(0)
  })

  it('renders pending on the server and opens nothing', async () => {
    const db = await openOrders('server-render', [[1, 'A-1', 'open']])

    const html = renderToString(createElement(OrderList, { database: db, status: 'open' }))
    await pause(QUIET_PERIOD_MS)

    expect(html).toBe('<p>pending</p>')
    expect(log.returned()).toEqual([])
  })

  it('shows the error state for a statement that cannot be kept current and for a registered read on a local database', async () => {
    const db = await openOrders('errors', [[1, 'A-1', 'open']])

    const openOrdersRead = operationRef<Record<string, never>, Order>('openOrders')

    function Statement({ sql }: { sql: string }): ReactElement {
      return createElement('p', null, describeState(useLiveQuery<Order>(db, sql)))
    }

    function RegisteredRead(): ReactElement {
      return createElement('p', null, describeState(useLiveQuery(db, openOrdersRead, {})))
    }

    const aggregate = mount(createElement(Statement, { sql: 'SELECT count(*) AS n FROM orders' }))
    const missing = mount(createElement(Statement, { sql: 'SELECT nope FROM orders ORDER BY id' }))
    const registered = mount(createElement(RegisteredRead))

    await shows(aggregate, 'error CDC_ERROR')
    await shows(missing, 'error QUERY_ERROR')
    await shows(registered, 'error CDC_ERROR')
  })
})

describe('useCommand over a local database', () => {
  it('returns one callback across renders, and the write it runs reaches the file and the live rows', async () => {
    const db = await openOrders('command', [[1, 'A-1', 'open']])
    const seen: ((params?: Params) => Promise<unknown>)[] = []
    let rerender = (): void => {}

    function Orders(): ReactElement {
      const [, setTick] = useState(0)
      useEffect(() => {
        rerender = () => setTick(tick => tick + 1)
      }, [])
      const placeOrder = useCommand(db, INSERT)
      seen.push(placeOrder)
      const state = useLiveQuery<Order>(db, BY_STATUS, ['open'], { rereadJitterMs: 0 })
      return createElement('p', null, describeState(state))
    }

    const page = mount(createElement(Orders))
    await shows(page, '1:A-1')
    rerender()
    await waitFor(() => seen.length >= 3, 'a second render')

    await seen[seen.length - 1]([2, 'A-2', 'open'])
    await shows(page, await oracleText(db, BY_STATUS, ['open']))

    expect(new Set(seen).size).toBe(1)
    expect(await db.query('SELECT id, reference FROM orders ORDER BY id')).toEqual([
      { id: 1, reference: 'A-1' },
      { id: 2, reference: 'A-2' },
    ])
  })
})
