// @vitest-environment jsdom

import { act, createElement, StrictMode, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { renderToString } from 'react-dom/server'
import { afterEach, beforeAll, describe, expect, it } from 'vitest'
import type { RemoteDatabase } from '../../client/database-proxy.js'
import type { Database } from '../../core/database.js'
import type { LiveQuery, LiveQueryOptions, LiveQueryState, LiveUpdate } from '../../core/live/types.js'
import { operationRef } from '../../core/operation-registry.js'
import type { LiveDatabase } from '../index.js'
import { useCommand, useLiveQuery } from '../index.js'

interface Order {
  id: number
  reference: string
}

const openOrders = operationRef<{ status: string }, Order>('openOrders')

class FakeLiveQuery implements LiveQuery<Order> {
  closed = false
  private state: LiveQueryState<Order>
  private readonly listeners = new Set<(update: LiveUpdate<Order>) => void>()

  constructor(rows: Order[]) {
    this.state = { status: 'ready', rows, revalidating: false }
  }

  getState(): LiveQueryState<Order> {
    return this.state
  }

  subscribe(listener: (update: LiveUpdate<Order>) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close(): Promise<void> {
    this.closed = true
  }

  push(rows: Order[]): void {
    this.state = { status: 'ready', rows, revalidating: false }
    for (const listener of [...this.listeners]) listener({ kind: 'rows' })
  }
}

interface FakeDatabase extends LiveDatabase {
  readonly opened: { name: string; args: unknown; options: LiveQueryOptions | undefined }[]
  readonly queries: FakeLiveQuery[]
}

function createDatabase(rows: Order[], hold?: { release: () => void }): FakeDatabase {
  const opened: { name: string; args: unknown; options: LiveQueryOptions | undefined }[] = []
  const queries: FakeLiveQuery[] = []
  let gate: Promise<void> | null = null

  if (hold !== undefined) {
    gate = new Promise<void>(resolve => {
      hold.release = resolve
    })
  }

  return {
    opened,
    queries,
    async live(operation, args, options) {
      opened.push({
        name: typeof operation === 'string' ? operation : operation.name,
        args,
        options: options as LiveQueryOptions | undefined,
      })
      if (gate !== null) await gate
      const query = new FakeLiveQuery(rows)
      queries.push(query)
      return query as unknown as LiveQuery<never>
    },
  }
}

const roots: { unmount: () => void }[] = []

beforeAll(() => {
  ;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true
})

function render(element: ReturnType<typeof createElement>): HTMLElement {
  const container = document.createElement('div')
  document.body.appendChild(container)
  const root = createRoot(container)
  roots.push(root)
  act(() => {
    root.render(element)
  })
  return container
}

afterEach(() => {
  for (const root of roots.splice(0)) {
    act(() => {
      root.unmount()
    })
  }
  document.body.innerHTML = ''
})

describe('useLiveQuery', () => {
  it('renders the rows and re-renders when they change', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      if (state.status !== 'ready') return createElement('p', null, state.status)
      return createElement('p', null, state.rows.map(order => order.reference).join(','))
    }

    const container = render(createElement(OrderCount))
    await act(async () => {})
    expect(container.textContent).toBe('A-1')

    await act(async () => {
      database.queries[0].push([
        { id: 1, reference: 'A-1' },
        { id: 2, reference: 'A-2' },
      ])
    })
    expect(container.textContent).toBe('A-1,A-2')
  })

  it('opens one subscription when the arguments are an inline object', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])
    let forceRender = (): void => {}

    function OrderCount(): ReturnType<typeof createElement> {
      const [, setTick] = useState(0)
      forceRender = () => setTick(tick => tick + 1)
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      return createElement('p', null, state.status)
    }

    render(createElement(OrderCount))
    await act(async () => {})
    await act(async () => {
      forceRender()
    })
    await act(async () => {
      forceRender()
    })

    expect(database.opened).toHaveLength(1)
  })

  it('shows both readers of one query the same rows in a frame', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])
    const shared = new FakeLiveQuery([{ id: 1, reference: 'A-1' }])
    const singleQuery: LiveDatabase = {
      live: async () => shared as unknown as LiveQuery<never>,
    }

    function Reader({ label }: { label: string }): ReturnType<typeof createElement> {
      const state = useLiveQuery(singleQuery, openOrders, { status: 'open' })
      const count = state.status === 'ready' ? state.rows.length : -1
      return createElement('span', null, `${label}:${count}`)
    }

    const container = render(
      createElement('div', null, createElement(Reader, { label: 'left' }), createElement(Reader, { label: 'right' })),
    )
    await act(async () => {})
    expect(container.textContent).toBe('left:1right:1')

    await act(async () => {
      shared.push([
        { id: 1, reference: 'A-1' },
        { id: 2, reference: 'A-2' },
      ])
    })
    expect(container.textContent).toBe('left:2right:2')
    expect(database.opened).toHaveLength(0)
  })

  it('opens nothing while disabled', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' }, { enabled: false })
      return createElement('p', null, state.status)
    }

    const container = render(createElement(OrderCount))
    await act(async () => {})

    expect(container.textContent).toBe('pending')
    expect(database.opened).toHaveLength(0)
  })

  it('closes the query when the component unmounts', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      return createElement('p', null, state.status)
    }

    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    act(() => {
      root.render(createElement(OrderCount))
    })
    await act(async () => {})
    expect(database.queries[0].closed).toBe(false)

    act(() => {
      root.unmount()
    })
    expect(database.queries[0].closed).toBe(true)
  })

  it('closes a query that arrives after the component unmounted', async () => {
    const hold = { release: (): void => {} }
    const database = createDatabase([{ id: 1, reference: 'A-1' }], hold)

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      return createElement('p', null, state.status)
    }

    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    act(() => {
      root.render(createElement(OrderCount))
    })

    act(() => {
      root.unmount()
    })
    await act(async () => {
      hold.release()
    })

    expect(database.queries[0].closed).toBe(true)
  })

  it('leaves one query open under a double-invoked mount', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      return createElement('p', null, state.status)
    }

    render(createElement(StrictMode, null, createElement(OrderCount)))
    await act(async () => {})

    const open = database.queries.filter(query => !query.closed)
    expect(open).toHaveLength(1)
  })

  it('closes the first query and opens the second when the arguments change', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])
    let setStatus = (_status: string): void => {}

    function OrderCount(): ReturnType<typeof createElement> {
      const [status, update] = useState('open')
      setStatus = update
      const state = useLiveQuery(database, openOrders, { status })
      return createElement('p', null, state.status)
    }

    render(createElement(OrderCount))
    await act(async () => {})
    await act(async () => {
      setStatus('cancelled')
    })

    expect(database.opened.map(entry => entry.args)).toEqual([{ status: 'open' }, { status: 'cancelled' }])
    expect(database.queries[0].closed).toBe(true)
    expect(database.queries[1].closed).toBe(false)
  })

  it('keeps one subscription when an argument is a big integer or a blob', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])
    let forceRender = (): void => {}

    function OrderCount(): ReturnType<typeof createElement> {
      const [, setTick] = useState(0)
      forceRender = () => setTick(tick => tick + 1)
      const state = useLiveQuery(database, openOrders, {
        account: 9007199254740993n,
        token: new Uint8Array([1, 2, 3]),
      } as never)
      return createElement('p', null, state.status)
    }

    render(createElement(OrderCount))
    await act(async () => {})
    await act(async () => {
      forceRender()
    })

    expect(database.opened).toHaveLength(1)
  })

  it('closes the query when it is disabled after opening', async () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])
    let disable = (): void => {}

    function OrderCount(): ReturnType<typeof createElement> {
      const [enabled, setEnabled] = useState(true)
      disable = () => setEnabled(false)
      const state = useLiveQuery(database, openOrders, { status: 'open' }, { enabled })
      return createElement('p', null, state.status)
    }

    const container = render(createElement(OrderCount))
    await act(async () => {})
    expect(container.textContent).toBe('ready')

    await act(async () => {
      disable()
    })

    expect(container.textContent).toBe('pending')
    expect(database.queries[0].closed).toBe(true)
    expect(database.opened).toHaveLength(1)
  })

  it('reports pending when rendered on a server', () => {
    const database = createDatabase([{ id: 1, reference: 'A-1' }])

    function OrderCount(): ReturnType<typeof createElement> {
      const state = useLiveQuery(database, openOrders, { status: 'open' })
      return createElement('p', null, state.status)
    }

    expect(renderToString(createElement(OrderCount))).toContain('pending')
    expect(database.opened).toHaveLength(0)
  })
})

describe('useCommand', () => {
  it('returns one callable across renders and runs the operation', async () => {
    const calls: { name: string; args: unknown }[] = []
    const database = {
      execute: async (operation: string | { name: string }, args?: { reference: string }) => {
        calls.push({ name: typeof operation === 'string' ? operation : operation.name, args })
        return [{ changes: 1, lastInsertRowId: 4 }]
      },
    }
    const cancelOrder = operationRef<{ reference: string }, never>('cancelOrder')
    const seen: ((args?: { reference: string }) => Promise<unknown>)[] = []
    let forceRender = (): void => {}

    function CancelControl(): ReturnType<typeof createElement> {
      const [, setTick] = useState(0)
      forceRender = () => setTick(tick => tick + 1)
      const cancel = useCommand(database, cancelOrder)
      seen.push(cancel)
      return createElement('span', null, 'cancel')
    }

    render(createElement(CancelControl))
    await act(async () => {
      forceRender()
    })

    expect(seen[0]).toBe(seen[1])
    await seen[0]({ reference: 'A-1' })
    expect(calls).toEqual([{ name: 'cancelOrder', args: { reference: 'A-1' } }])
  })
})

describe('LiveDatabase', () => {
  it('is satisfied by the remote and core database types', () => {
    type RemoteIsLiveDatabase = RemoteDatabase extends LiveDatabase ? true : false
    type CoreIsLiveDatabase = Database extends LiveDatabase ? true : false

    const satisfied: [RemoteIsLiveDatabase, CoreIsLiveDatabase] = [true, true]

    expect(satisfied).toEqual([true, true])
  })
})
