import { describe, expect, it } from 'vitest'
import type { LiveQuery, LiveQueryState, LiveUpdate } from '../../core/live/types.js'
import type { Subscription } from '../../core/types.js'
import type { OperationSource } from '../operation-lookup.js'
import type { WSConnection } from '../ws-connection.js'
import type { ConnectionState } from '../ws-handler.js'
import type { WSLiveDeps } from '../ws-live.js'
import { handleLiveSubscribeMessage } from '../ws-live.js'

type Row = Record<string, unknown>

class ControllableLiveQuery implements LiveQuery<Row> {
  closed = false
  private state: LiveQueryState<Row> = { status: 'ready', rows: [{ id: 1 }], revalidating: false }
  private readonly listeners = new Set<(update: LiveUpdate<Row>) => void>()

  getState(): LiveQueryState<Row> {
    return this.state
  }

  subscribe(listener: (update: LiveUpdate<Row>) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close(): Promise<void> {
    this.closed = true
  }

  get listenerCount(): number {
    return this.listeners.size
  }

  fail(message: string): void {
    this.state = { status: 'error', error: new Error(message) }
    for (const listener of [...this.listeners]) listener({ kind: 'error' })
  }
}

interface Recorded {
  errors: { id: string; code: string; message: string }[]
  rows: unknown[][]
}

function createDeps(recorded: Recorded): WSLiveDeps {
  const operations: OperationSource = {
    digest: undefined,
    resolve: () => ({ ok: true, statements: [{ sql: 'SELECT id FROM orders' }] }),
  }

  return {
    operations,
    sendSubscribedRows: (_conn, _id, rows) => {
      recorded.rows.push(rows)
    },
    sendLive: () => {},
    sendError: (_conn, id, code, message) => {
      recorded.errors.push({ id, code, message })
    },
    sendSirannonError: (_conn, id, err) => {
      recorded.errors.push({ id, code: 'INTERNAL_ERROR', message: String(err) })
    },
  }
}

function createState(query: LiveQuery<Row>): ConnectionState {
  return {
    databaseId: 'shop',
    database: { readOnly: false, path: '/tmp/shop.db', live: async () => query },
    executionTarget: {},
    identity: undefined,
    subscriptions: new Map<string, Subscription>(),
    deviceStreams: new Map(),
    overloaded: false,
  } as unknown as ConnectionState
}

const connection = {} as WSConnection

describe('a live query that fails after it is established', () => {
  it('closes the query, forgets the subscription, and reports the failure once', async () => {
    const query = new ControllableLiveQuery()
    const recorded: Recorded = { errors: [], rows: [] }
    const state = createState(query)

    await handleLiveSubscribeMessage(createDeps(recorded), connection, state, {}, 'live-1', 'everyOrder')
    expect(recorded.rows).toEqual([[{ id: 1 }]])
    expect(state.subscriptions.has('live-1')).toBe(true)

    query.fail('the probe went away')

    expect(recorded.errors).toEqual([{ id: 'live-1', code: 'CDC_ERROR', message: 'the probe went away' }])
    expect(query.closed).toBe(true)
    expect(query.listenerCount).toBe(0)
    expect(state.subscriptions.has('live-1')).toBe(false)
  })

  it('does not report a second failure after the first tore the query down', async () => {
    const query = new ControllableLiveQuery()
    const recorded: Recorded = { errors: [], rows: [] }
    const state = createState(query)

    await handleLiveSubscribeMessage(createDeps(recorded), connection, state, {}, 'live-1', 'everyOrder')
    query.fail('first')
    query.fail('second')

    expect(recorded.errors).toHaveLength(1)
  })
})
