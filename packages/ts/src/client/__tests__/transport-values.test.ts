import { describe, expect, it, vi } from 'vitest'
import { HttpTransport } from '../transport/http.js'
import { WebSocketTransport } from '../transport/ws.js'

interface FakeSocket extends EventTarget {
  readyState: number
  readonly sent: string[]
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

async function until(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`until timed out after ${timeout}ms`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

describe('WebSocketTransport value encoding', () => {
  it('encodes BigInt and Buffer params into tagged envelopes and decodes envelope rows back', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        requestTimeout: 1000,
      })

      const pending = transport.query('SELECT * FROM ledgers WHERE balance = ? AND payload = ?', [
        9007199254740993n,
        Buffer.from([0x00, 0xff]),
      ])

      await until(() => sockets.length > 0 && sockets[0].sent.length > 0)
      const frame = JSON.parse(sockets[0].sent[0]) as Record<string, unknown>
      expect(frame.params).toEqual([{ __sirannon_int: '9007199254740993' }, { __sirannon_blob: '00FF' }])

      sockets[0].deliver({
        type: 'result',
        id: frame.id,
        data: {
          rows: [{ id: 1, balance: { __sirannon_int: '9007199254740993' }, payload: { __sirannon_blob: '00FF' } }],
        },
      })

      const { rows } = await pending
      expect(rows[0].balance).toBe(9007199254740993n)
      const payload = rows[0].payload
      expect(Buffer.isBuffer(payload) || payload instanceof Uint8Array).toBe(true)
      expect(Array.from(payload as Uint8Array)).toEqual([0x00, 0xff])

      transport.close()
    } finally {
      restore()
    }
  })
})

describe('WebSocketTransport filter encoding', () => {
  it('encodes BigInt filter values into tagged envelopes on subscribe', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        requestTimeout: 1000,
      })

      const pending = transport.subscribe('ledgers', { balance: 9007199254740993n }, () => {})

      await until(() => sockets.length > 0 && sockets[0].sent.length > 0)
      const frame = JSON.parse(sockets[0].sent[0]) as Record<string, unknown>
      expect(frame.type).toBe('subscribe')
      expect(frame.filter).toEqual({ balance: { __sirannon_int: '9007199254740993' } })

      sockets[0].deliver({ type: 'subscribed', id: frame.id, seq: '0' })
      const sub = await pending
      sub.unsubscribe()
      transport.close()
    } finally {
      restore()
    }
  })
})

describe('HttpTransport value encoding', () => {
  it('encodes BigInt params into tagged envelopes and decodes envelope rows back', async () => {
    const requests: { url: string; body: string }[] = []
    const originalFetch = globalThis.fetch
    vi.stubGlobal('fetch', async (url: string, init: { body: string }) => {
      requests.push({ url, body: init.body })
      return new Response(JSON.stringify({ rows: [{ id: 1, balance: { __sirannon_int: '-9223372036854775808' } }] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    })

    try {
      const transport = new HttpTransport('http://localhost:1234/db/test')
      const { rows } = await transport.query('SELECT * FROM ledgers WHERE balance = ?', [-9223372036854775808n])

      const sentBody = JSON.parse(requests[0].body) as Record<string, unknown>
      expect(sentBody.params).toEqual([{ __sirannon_int: '-9223372036854775808' }])
      expect(rows[0].balance).toBe(-9223372036854775808n)
      transport.close()
    } finally {
      vi.stubGlobal('fetch', originalFetch)
    }
  })
})
