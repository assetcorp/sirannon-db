import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ChangeEvent } from '../../core/types.js'
import { Sirannon } from '../../core/sirannon.js'
import { createServer } from '../../server/server.js'
import type { SirannonServer } from '../../server/server.js'
import { createWSHandler } from '../../server/ws-handler.js'
import type { WSHandler, WSConnection } from '../../server/ws-handler.js'
import { SirannonClient } from '../client.js'
import { RemoteSubscriptionBuilderImpl } from '../subscription.js'
import { WebSocketTransport } from '../transport/ws.js'
import { RemoteError } from '../types.js'

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe('RemoteSubscriptionBuilder', () => {
  it('passes filter conditions to the transport subscribe', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    const callback = () => {}
    await builder.filter({ name: 'Alice' }).subscribe(callback)

    expect(subscribeFn).toHaveBeenCalledWith(
      'users',
      { name: 'Alice' },
      callback,
    )
  })

  it('merges multiple filter calls', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    await builder
      .filter({ name: 'Alice' })
      .filter({ age: 30 })
      .subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith(
      'users',
      { name: 'Alice', age: 30 },
      expect.any(Function),
    )
  })

  it('passes undefined filter when no conditions are set', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('orders', transport)
    await builder.subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith(
      'orders',
      undefined,
      expect.any(Function),
    )
  })
})

describe('WebSocket subscription integration', () => {
  let tempDir: string
  let sirannon: Sirannon
  let wsHandler: WSHandler

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sub-'))
    sirannon = new Sirannon()
  })

  afterEach(() => {
    wsHandler?.close()
    sirannon.shutdown()
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('subscribes and receives insert events via mock WS handler', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'sub.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)
    const events: ChangeEvent[] = []

    // Simulate a WS connection using the mock approach from server tests
    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    const subMsg = JSON.parse(conn.messages[conn.messages.length - 1])
    expect(subMsg.type).toBe('subscribed')

    // Insert a row and wait for CDC polling
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.type).toBe('insert')
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('receives filtered events', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'filter.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
        filter: { name: 'Alice' },
      }),
    )

    db.execute("INSERT INTO users (name) VALUES ('Bob')")
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('stops receiving events after unsubscribe', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'unsub.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    // Verify subscription is active
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    let changeCount = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change').length
    expect(changeCount).toBe(1)

    // Unsubscribe
    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'unsubscribe',
      }),
    )

    // Insert after unsubscribe
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    changeCount = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change').length
    expect(changeCount).toBe(1) // Still 1, no new events
  })

  it('receives update and delete events', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'upddel.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    db.execute("DELETE FROM users WHERE name = 'Alice'")
    await wait(200)

    const events = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change')
      .map(m => m.event)

    expect(events).toHaveLength(3)
    expect(events[0].type).toBe('insert')
    expect(events[1].type).toBe('update')
    expect(events[1].oldRow.age).toBe(30)
    expect(events[1].row.age).toBe(31)
    expect(events[2].type).toBe('delete')
  })

  it('multiple subscriptions on the same table', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'multi.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-a',
        type: 'subscribe',
        table: 'users',
        filter: { name: 'Alice' },
      }),
    )
    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-b',
        type: 'subscribe',
        table: 'users',
      }),
    )

    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    const changes = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change')

    // sub-a should get 1 event (Alice only), sub-b should get 2 events (all)
    const subAChanges = changes.filter(m => m.id === 'sub-a')
    const subBChanges = changes.filter(m => m.id === 'sub-b')
    expect(subAChanges).toHaveLength(1)
    expect(subBChanges).toHaveLength(2)
  })

  it('change events include seq as string and timestamp', async () => {
    const db = sirannon.open('mydb', join(tempDir, 'seq.db'))
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const change = conn.messages
      .map(m => JSON.parse(m))
      .find(m => m.type === 'change')

    expect(typeof change.event.seq).toBe('string')
    expect(typeof change.event.timestamp).toBe('number')
    expect(Number(change.event.seq)).toBeGreaterThan(0)
  })
})

describe('WebSocketTransport', () => {
  it('rejects transactions', async () => {
    const transport = new WebSocketTransport('ws://localhost:1234/db/test')
    await expect(
      transport.transaction([{ sql: 'SELECT 1' }]),
    ).rejects.toThrow('Transactions are not supported over WebSocket')
    transport.close()
  })

  it('rejects operations after close', async () => {
    const transport = new WebSocketTransport('ws://localhost:1234/db/test')
    transport.close()
    await expect(transport.query('SELECT 1')).rejects.toThrow(
      'Transport is closed',
    )
  })
})

// Helper: mock WebSocket connection for the WSHandler (same pattern as server tests)
interface MockWSConnection extends WSConnection {
  messages: string[]
  closed: boolean
  closeCode?: number
  closeReason?: string
}

function createMockConnection(): MockWSConnection {
  const conn: MockWSConnection = {
    messages: [],
    closed: false,
    closeCode: undefined,
    closeReason: undefined,
    send(data: string) {
      conn.messages.push(data)
    },
    close(code?: number, reason?: string) {
      conn.closed = true
      conn.closeCode = code
      conn.closeReason = reason
    },
  }
  return conn
}
