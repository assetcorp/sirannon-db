import { createElement, type ReactElement, StrictMode, useEffect, useState } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { renderToString } from 'react-dom/server'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/client.js'
import type { RemoteDatabase } from '../../../client/database-proxy.js'
import { type OperationArguments, type OperationRegistry, operationRef } from '../../../core/operation-registry.js'
import { type LiveQueryState, useCommand, useLiveQuery } from '../../../react/index.js'
import { createClientServerHarness } from '../lib/server-harness.js'

interface Member {
  id: number
  name: string
}

interface Frame {
  type?: string
  id?: string
  name?: string
}

const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 8_000
const QUIET_PERIOD_MS = 150

const members = operationRef<Record<string, never>, Member>('members')
const membersCalled = operationRef<{ name: string }, Member>('membersCalled')
const ledgerMatching = operationRef<{ account: bigint; token: Uint8Array }, { id: number }>('ledgerMatching')
const addMember = operationRef<{ name: string }, never>('addMember')
const addLedgerEntry = operationRef<{ id: number; account: bigint; token: Uint8Array }, never>('addLedgerEntry')

const operations: OperationRegistry = {
  testdb: {
    reads: {
      members: { statement: () => ({ sql: 'SELECT id, name FROM users ORDER BY id' }) },
      membersCalled: {
        args: ['name'],
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id, name FROM users WHERE name = ? ORDER BY id',
          params: [args.name],
        }),
      },
      ledgerMatching: {
        args: ['account', 'token'],
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id FROM ledger WHERE account = ? AND token = ? ORDER BY id',
          params: [args.account, args.token],
        }),
      },
    },
    writes: {
      addMember: {
        args: ['name'],
        statements: (args: OperationArguments) => ({ sql: 'INSERT INTO users (name) VALUES (?)', params: [args.name] }),
      },
      addLedgerEntry: {
        args: ['id', 'account', 'token'],
        statements: (args: OperationArguments) => ({
          sql: 'INSERT INTO ledger (id, account, token) VALUES (?, ?, ?)',
          params: [args.id, args.account, args.token],
        }),
      },
    },
  },
}

interface Mounted {
  container: HTMLElement
  root: Root
  unmounted: boolean
}

const harness = createClientServerHarness()
const NativeWebSocket = globalThis.WebSocket
let frames: Frame[] = []
let holdingReplies = false
let heldReplies: (() => void)[] = []
const clients: SirannonClient[] = []
const mounted: Mounted[] = []

class RecordingWebSocket extends NativeWebSocket {
  override send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    frames.push(JSON.parse(String(data)) as Frame)
    super.send(data)
  }

  override addEventListener(type: string, listener: EventListenerOrEventListenerObject, options?: unknown): void {
    if (type !== 'message' || typeof listener !== 'function') {
      super.addEventListener(type, listener, options as AddEventListenerOptions)
      return
    }
    const gated = (event: Event) => {
      if (holdingReplies) heldReplies.push(() => listener.call(this, event))
      else listener.call(this, event)
    }
    super.addEventListener(type, gated, options as AddEventListenerOptions)
  }
}

const JSDOM_MODULE = 'jsdom'
const pageGlobals = globalThis as unknown as { window?: unknown; document?: Document; WebSocket: typeof WebSocket }

beforeAll(async () => {
  const { JSDOM } = (await import(JSDOM_MODULE)) as {
    JSDOM: new (html: string) => { window: { document: Document } }
  }
  const page = new JSDOM('<!doctype html><html><body></body></html>')
  pageGlobals.window = page.window
  pageGlobals.document = page.window.document
})

afterAll(async () => {
  await pause(QUIET_PERIOD_MS)
  delete pageGlobals.window
  delete pageGlobals.document
})

beforeEach(async () => {
  frames = []
  holdingReplies = false
  heldReplies = []
  pageGlobals.WebSocket = RecordingWebSocket
  await harness.restart({ acceptSql: true, operations })
})

afterEach(() => {
  for (const entry of mounted.splice(0)) if (!entry.unmounted) entry.root.unmount()
  document.body.innerHTML = ''
  for (const client of clients.splice(0)) client.close()
  pageGlobals.WebSocket = NativeWebSocket
})

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function waitFor(condition: () => boolean, what: () => string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what()}`)
    await pause(WAIT_STEP_MS)
  }
}

function connect(transport?: 'http'): RemoteDatabase {
  const client = new SirannonClient(harness.baseUrl, { reconnectInterval: 50, ...(transport ? { transport } : {}) })
  clients.push(client)
  return client.database('testdb')
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

async function shows(entry: Mounted, text: string): Promise<void> {
  await waitFor(
    () => entry.container.textContent === text,
    () => `the page to show '${text}', showing '${entry.container.textContent}'`,
  )
}

function liveSubscriptions(): Frame[] {
  const closed = new Set(frames.filter(frame => frame.type === 'unsubscribe').map(frame => frame.id))
  return frames.filter(frame => frame.type === 'subscribe' && frame.name !== undefined && !closed.has(frame.id))
}

function describeState(state: LiveQueryState<Member>): string {
  if (state.status === 'ready') {
    const rows = state.rows.map(row => `${row.id}:${row.name}`).join(',') || 'none'
    return state.revalidating ? `stale ${rows}` : rows
  }
  if (state.status === 'error') return `error ${(state.error as { code?: string }).code ?? state.error.message}`
  return state.status
}

async function oracleText(db: RemoteDatabase): Promise<string> {
  const rows = await db.query(members, {})
  return rows.map(row => `${row.id}:${row.name}`).join(',') || 'none'
}

function MemberList({ database, label = '' }: { database: RemoteDatabase; label?: string }): ReactElement {
  return createElement('p', null, `${label}${describeState(useLiveQuery(database, members, {}))}`)
}

describe('useLiveQuery over a remote database', () => {
  it('renders the rows of a registered read in every reader and re-renders after each registered write, as a plain read returns them', async () => {
    const db = connect()
    const writer = connect()
    const page = mount(
      createElement(
        'div',
        null,
        createElement(MemberList, { database: db, label: 'left ' }),
        createElement(MemberList, { database: db, label: ' right ' }),
      ),
    )
    const expected = async () => {
      const text = await oracleText(writer)
      return `left ${text} right ${text}`
    }

    await shows(page, await expected())
    for (const name of ['Bilal', 'Chidi', 'Dara']) {
      await writer.execute(addMember, { name })
      await shows(page, await expected())
    }
    await writer.execute("UPDATE users SET name = 'Ama' WHERE id = 1")
    await writer.execute('DELETE FROM users WHERE id = 3')
    await shows(page, await expected())
  })

  it('keeps one server subscription per argument set across re-renders and StrictMode, BigInt and BLOB arguments included', async () => {
    const db = connect()
    await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, account INTEGER NOT NULL, token BLOB NOT NULL)')
    await db.execute(addLedgerEntry, { id: 1, account: 9007199254740993n, token: new Uint8Array([1, 2, 3]) })
    await db.execute(addLedgerEntry, { id: 2, account: 5n, token: new Uint8Array([9]) })
    let rerender = (): void => {}
    let switchAccount = (): void => {}

    function Ledger(): ReactElement {
      const [, setTick] = useState(0)
      const [second, setSecond] = useState(false)
      useEffect(() => {
        rerender = () => setTick(tick => tick + 1)
        switchAccount = () => setSecond(true)
      }, [])
      const args = second
        ? { account: 5n, token: new Uint8Array([9]) }
        : { account: 9007199254740993n, token: new Uint8Array([1, 2, 3]) }
      const state = useLiveQuery(db, ledgerMatching, args)
      return createElement('p', null, state.status === 'ready' ? state.rows.map(row => row.id).join(',') : state.status)
    }

    const page = mount(createElement(StrictMode, null, createElement(Ledger)))
    await shows(page, '1')
    for (let round = 0; round < 3; round++) {
      rerender()
      await pause(10)
    }
    await pause(QUIET_PERIOD_MS)
    expect(liveSubscriptions()).toHaveLength(1)

    await db.execute(addLedgerEntry, { id: 3, account: 9007199254740993n, token: new Uint8Array([1, 2, 3]) })
    await shows(page, '1,3')
    switchAccount()
    await shows(page, '2')
    await db.execute(addLedgerEntry, { id: 4, account: 5n, token: new Uint8Array([9]) })
    await shows(page, '2,4')
    await pause(QUIET_PERIOD_MS)

    const open = liveSubscriptions()
    expect(open).toHaveLength(1)
    expect(open[0]).toMatchObject({ name: 'ledgerMatching' })
  })

  it('closes the server subscription on unmount, including one whose first rows arrive after the unmount', async () => {
    const db = connect()
    const settled = mount(createElement(MemberList, { database: db }))
    await shows(settled, await oracleText(db))
    expect(liveSubscriptions()).toHaveLength(1)
    unmount(settled)
    await waitFor(
      () => liveSubscriptions().length === 0,
      () => 'the unmounted query to unsubscribe',
    )

    holdingReplies = true
    const early = mount(createElement(MemberList, { database: db }))
    await waitFor(
      () => liveSubscriptions().length === 1,
      () => 'the second query to subscribe',
    )
    unmount(early)
    holdingReplies = false
    for (const deliver of heldReplies.splice(0)) deliver()
    await waitFor(
      () => liveSubscriptions().length === 0,
      () => `the late query to unsubscribe, with frames ${JSON.stringify(frames)}`,
    )
    await connect().execute(addMember, { name: 'Esi' })
    await pause(QUIET_PERIOD_MS)

    expect(early.container.textContent).toBe('')
    expect(liveSubscriptions()).toEqual([])
  })

  it('stays closed while disabled, subscribes once enabled, and unsubscribes when disabled again', async () => {
    const db = connect()
    let setEnabled = (_enabled: boolean): void => {}

    function Toggle(): ReactElement {
      const [enabled, update] = useState(false)
      useEffect(() => {
        setEnabled = update
      }, [])
      return createElement('p', null, describeState(useLiveQuery(db, members, {}, { enabled })))
    }

    const page = mount(createElement(Toggle))
    await shows(page, 'pending')
    await pause(QUIET_PERIOD_MS)
    expect(liveSubscriptions()).toEqual([])

    setEnabled(true)
    await shows(page, await oracleText(db))
    expect(liveSubscriptions()).toHaveLength(1)

    setEnabled(false)
    await shows(page, 'pending')
    await waitFor(
      () => liveSubscriptions().length === 0,
      () => 'the disabled query to unsubscribe',
    )
  })

  it('shows its rows as stale while the server is down, then the fresh rows once it is back', async () => {
    const db = connect()
    const page = mount(createElement(MemberList, { database: db }))
    const before = await oracleText(db)
    await shows(page, before)

    await harness.stop()
    await shows(page, `stale ${before}`)
    await harness.start({ acceptSql: true, operations })
    const writer = connect()
    await writer.execute(addMember, { name: 'Femi' })

    await shows(page, await oracleText(writer))
  })

  it('shows the error state for a read the server does not know and over the HTTP transport', async () => {
    const db = connect()
    const overHttp = connect('http')

    function Unknown(): ReactElement {
      return createElement('p', null, describeState(useLiveQuery<Member>(db, 'archivedMembers')))
    }

    const unknown = mount(createElement(Unknown))
    const http = mount(createElement(MemberList, { database: overHttp }))

    await shows(unknown, 'error UNKNOWN_QUERY')
    await shows(http, 'error TRANSPORT_ERROR')
  })

  it('renders pending on the server and subscribes to nothing', async () => {
    const db = connect()

    const html = renderToString(createElement(MemberList, { database: db }))
    await pause(QUIET_PERIOD_MS)

    expect(html).toBe('<p>pending</p>')
    expect(frames).toEqual([])
  })
})

describe('useCommand over a remote database', () => {
  it('returns one callback across renders, and the registered write it runs reaches the live rows', async () => {
    const db = connect()
    const seen: ((args: { name: string }) => Promise<unknown>)[] = []
    let rerender = (): void => {}

    function Members(): ReactElement {
      const [, setTick] = useState(0)
      useEffect(() => {
        rerender = () => setTick(tick => tick + 1)
      }, [])
      const add = useCommand(db, addMember)
      seen.push(add)
      return createElement('p', null, describeState(useLiveQuery(db, membersCalled, { name: 'Gifty' })))
    }

    const page = mount(createElement(Members))
    await shows(page, 'none')
    rerender()
    await waitFor(
      () => seen.length >= 3,
      () => 'a second render',
    )

    await seen[seen.length - 1]({ name: 'Gifty' })
    const rows = await db.query(membersCalled, { name: 'Gifty' })
    await shows(page, rows.map(row => `${row.id}:${row.name}`).join(','))

    expect(rows).toHaveLength(1)
    expect(new Set(seen).size).toBe(1)
  })
})
