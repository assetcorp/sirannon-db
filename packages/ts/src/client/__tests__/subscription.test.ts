import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { ChangeEvent } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { WSConnection, WSHandler, WSSendOutcome } from '../../server/ws-handler.js'
import { createWSHandler } from '../../server/ws-handler.js'
import { RemoteSubscriptionBuilderImpl } from '../subscription.js'
import { WebSocketTransport } from '../transport/ws.js'

const driver = betterSqlite3()

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
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    const callback = () => {}
    await builder.filter({ name: 'Alice' }).subscribe(callback)

    expect(subscribeFn).toHaveBeenCalledWith('users', { name: 'Alice' }, callback, undefined)
  })

  it('merges multiple filter calls', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    await builder
      .filter({ name: 'Alice' })
      .filter({ age: 30 })
      .subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith('users', { name: 'Alice', age: 30 }, expect.any(Function), undefined)
  })

  it('passes undefined filter when no conditions are set', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('orders', transport)
    await builder.subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith('orders', undefined, expect.any(Function), undefined)
  })
})

describe('WebSocket subscription integration', () => {
  let tempDir: string
  let sirannon: Sirannon
  let wsHandler: WSHandler

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sub-'))
    sirannon = new Sirannon({ driver })
  })

  afterEach(async () => {
    await wsHandler?.close()
    await sirannon.shutdown()
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('subscribes and receives insert events via mock WS handler', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'sub.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)
    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    const subMsg = JSON.parse(conn.messages[conn.messages.length - 1])
    expect(subMsg.type).toBe('subscribed')

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.type).toBe('insert')
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('receives filtered events', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'filter.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
        filter: { name: 'Alice' },
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('stops receiving events after unsubscribe', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'unsub.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    let changeCount = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change').length
    expect(changeCount).toBe(1)

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'unsubscribe',
      }),
    )

    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    changeCount = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change').length
    expect(changeCount).toBe(1)
  })

  it('receives update and delete events', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'upddel.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    await db.execute("DELETE FROM users WHERE name = 'Alice'")
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
    const db = await sirannon.open('mydb', join(tempDir, 'multi.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

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

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    const changes = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    const subAChanges = changes.filter(m => m.id === 'sub-a')
    const subBChanges = changes.filter(m => m.id === 'sub-b')
    expect(subAChanges).toHaveLength(1)
    expect(subBChanges).toHaveLength(2)
  })

  it('change events include seq as string and timestamp', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'seq.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon)

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const change = conn.messages.map(m => JSON.parse(m)).find(m => m.type === 'change')

    expect(typeof change.event.seq).toBe('string')
    expect(typeof change.event.timestamp).toBe('number')
    expect(Number(change.event.seq)).toBeGreaterThan(0)
  })
})

describe('WebSocketTransport', () => {
  it('passes configured WebSocket protocols during the handshake', async () => {
    const originalWebSocket = globalThis.WebSocket
    const protocols = ['sirannon.demo.auth.token']
    const capturedConnections: Array<{ url: string; protocols?: string | string[] }> = []

    class ProtocolWebSocket extends EventTarget {
      static readonly CONNECTING = 0
      static readonly OPEN = 1
      static readonly CLOSING = 2
      static readonly CLOSED = 3

      readyState = ProtocolWebSocket.CONNECTING

      constructor(url: string | URL, requestedProtocols?: string | string[]) {
        super()
        capturedConnections.push({ url: String(url), protocols: requestedProtocols })
        queueMicrotask(() => {
          this.readyState = ProtocolWebSocket.OPEN
          this.dispatchEvent(new Event('open'))
        })
      }

      send(): void {}

      close(): void {
        this.readyState = ProtocolWebSocket.CLOSED
        this.dispatchEvent(new Event('close'))
      }
    }

    vi.stubGlobal('WebSocket', ProtocolWebSocket)

    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        protocols,
        requestTimeout: 1,
      })

      await expect(transport.query('SELECT 1')).rejects.toThrow('Request timed out after 1ms')
      expect(capturedConnections).toEqual([{ url: 'ws://localhost:1234/db/test', protocols }])
      transport.close()
    } finally {
      vi.stubGlobal('WebSocket', originalWebSocket)
    }
  })

  it('rejects transactions', async () => {
    const transport = new WebSocketTransport('ws://localhost:1234/db/test')
    await expect(transport.transaction([{ sql: 'SELECT 1' }])).rejects.toThrow(
      'Transactions are not supported over WebSocket',
    )
    transport.close()
  })

  it('rejects operations after close', async () => {
    const transport = new WebSocketTransport('ws://localhost:1234/db/test')
    transport.close()
    await expect(transport.query('SELECT 1')).rejects.toThrow('Transport is closed')
  })

  it('resumes from the last seen seq and reports a reset after reconnect', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const changes: ChangeEvent[] = []
      let resets = 0
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: true,
        reconnectInterval: 10,
        requestTimeout: 1000,
      })

      const subscribed = transport.subscribe('users', undefined, e => changes.push(e), {
        onReset: () => {
          resets++
        },
      })

      await until(() => firstSubscribeFrame(sockets[0]) !== undefined)
      const first = firstSubscribeFrame(sockets[0])
      expect(first?.sinceSeq).toBeUndefined()
      const subId = String(first?.id)
      sockets[0].deliver({ type: 'subscribed', id: subId, seq: '4' })
      await subscribed

      sockets[0].deliver({
        type: 'change',
        id: subId,
        event: { type: 'insert', table: 'users', row: { id: 1, name: 'Alice' }, seq: '5', timestamp: 1 },
      })
      await until(() => changes.length >= 1)
      expect(changes[0].seq).toBe(5n)

      sockets[0].close()
      await until(() => sockets.length >= 2 && firstSubscribeFrame(sockets[1]) !== undefined)
      const resumed = firstSubscribeFrame(sockets[1])
      expect(resumed?.sinceSeq).toBe('5')

      sockets[1].deliver({ type: 'subscribed', id: String(resumed?.id), seq: '5', resync: true })
      await until(() => resets >= 1)
      expect(resets).toBe(1)

      transport.close()
    } finally {
      restore()
    }
  })

  it('decodes tagged big-integer and blob envelopes on change rows into native values', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const changes: ChangeEvent[] = []
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        requestTimeout: 1000,
      })

      const subscribed = transport.subscribe('ledgers', undefined, e => changes.push(e))
      await until(() => firstSubscribeFrame(sockets[0]) !== undefined)
      const subId = String(firstSubscribeFrame(sockets[0])?.id)
      sockets[0].deliver({ type: 'subscribed', id: subId, seq: '1' })
      await subscribed

      sockets[0].deliver({
        type: 'change',
        id: subId,
        event: {
          type: 'update',
          table: 'ledgers',
          row: { id: 1, balance: { __sirannon_int: '9007199254740995' }, payload: { __sirannon_blob: '0001FFAB' } },
          oldRow: { id: 1, balance: { __sirannon_int: '9007199254740993' }, payload: null },
          seq: '2',
          timestamp: 1,
        },
      })
      await until(() => changes.length >= 1)

      const row = changes[0].row as Record<string, unknown>
      expect(row.balance).toBe(9007199254740995n)
      const blob = row.payload
      expect(Buffer.isBuffer(blob) || blob instanceof Uint8Array).toBe(true)
      expect(Array.from(blob as Uint8Array)).toEqual([0x00, 0x01, 0xff, 0xab])

      const oldRow = changes[0].oldRow as Record<string, unknown>
      expect(oldRow.balance).toBe(9007199254740993n)
      expect(oldRow.payload).toBeNull()

      transport.close()
    } finally {
      restore()
    }
  })

  it('resumes from the subscribed baseline even when no change was received', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: true,
        reconnectInterval: 10,
        requestTimeout: 1000,
      })

      const subscribed = transport.subscribe('users', undefined, () => {})
      await until(() => firstSubscribeFrame(sockets[0]) !== undefined)
      const subId = String(firstSubscribeFrame(sockets[0])?.id)
      sockets[0].deliver({ type: 'subscribed', id: subId, seq: '42' })
      await subscribed

      sockets[0].close()
      await until(() => sockets.length >= 2 && firstSubscribeFrame(sockets[1]) !== undefined)
      expect(firstSubscribeFrame(sockets[1])?.sinceSeq).toBe('42')

      transport.close()
    } finally {
      restore()
    }
  })
})

interface FakeSocket extends EventTarget {
  readyState: number
  readonly sent: string[]
  close(): void
  deliver(message: unknown): void
}

function installFakeWebSockets(): { sockets: FakeSocket[]; restore: () => void } {
  const originalWebSocket = globalThis.WebSocket
  const sockets: FakeSocket[] = []

  class FakeWebSocket extends EventTarget {
    static readonly CONNECTING = 0
    static readonly OPEN = 1
    static readonly CLOSING = 2
    static readonly CLOSED = 3

    readyState = FakeWebSocket.CONNECTING
    readonly sent: string[] = []

    constructor(readonly url: string | URL) {
      super()
      sockets.push(this)
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN
        this.dispatchEvent(new Event('open'))
      })
    }

    send(data: string): void {
      this.sent.push(data)
    }

    close(): void {
      this.readyState = FakeWebSocket.CLOSED
      this.dispatchEvent(new Event('close'))
    }

    deliver(message: unknown): void {
      const event = new Event('message') as Event & { data: string }
      event.data = JSON.stringify(message)
      this.dispatchEvent(event)
    }
  }

  vi.stubGlobal('WebSocket', FakeWebSocket)
  return { sockets, restore: () => vi.stubGlobal('WebSocket', originalWebSocket) }
}

function firstSubscribeFrame(socket: FakeSocket): Record<string, unknown> | undefined {
  return socket.sent.map(s => JSON.parse(s) as Record<string, unknown>).find(m => m.type === 'subscribe')
}

async function until(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`until timed out after ${timeout}ms`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

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
    send(data: string): WSSendOutcome {
      conn.messages.push(data)
      return 'sent'
    },
    close(code?: number, reason?: string) {
      conn.closed = true
      conn.closeCode = code
      conn.closeReason = reason
    },
  }
  return conn
}
