import { describe, expect, it, vi } from 'vitest'
import type { ChangeEvent } from '../../core/types.js'
import { WebSocketTransport } from '../transport/ws.js'
import { firstFrameOfType, firstSubscribeFrame, installFakeWebSockets, until } from './helpers.js'

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

  it('sends a transaction frame carrying every statement and resolves with the results', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        requestTimeout: 1000,
      })

      const pending = transport.transaction([
        { sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?', params: [25, 1] },
        { sql: 'INSERT INTO ledger (account_id, delta) VALUES (?, ?)', params: [1, -25] },
      ])

      await until(() => firstFrameOfType(sockets[0], 'transaction') !== undefined)
      const frame = firstFrameOfType(sockets[0], 'transaction')
      expect(frame?.statements).toEqual([
        { sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?', params: [25, 1] },
        { sql: 'INSERT INTO ledger (account_id, delta) VALUES (?, ?)', params: [1, -25] },
      ])

      sockets[0].deliver({
        type: 'result',
        id: String(frame?.id),
        data: {
          results: [
            { changes: 1, lastInsertRowId: 0 },
            { changes: 1, lastInsertRowId: 7 },
          ],
        },
      })

      await expect(pending).resolves.toEqual({
        results: [
          { changes: 1, lastInsertRowId: 0 },
          { changes: 1, lastInsertRowId: 7 },
        ],
      })
      transport.close()
    } finally {
      restore()
    }
  })

  it('rejects a transaction with the server error when the statements do not commit', async () => {
    const { sockets, restore } = installFakeWebSockets()
    try {
      const transport = new WebSocketTransport('ws://localhost:1234/db/test', {
        autoReconnect: false,
        requestTimeout: 1000,
      })

      const pending = transport.transaction([{ sql: 'INSERT INTO ledger (id) VALUES (?)', params: [1] }])
      await until(() => firstFrameOfType(sockets[0], 'transaction') !== undefined)
      const frame = firstFrameOfType(sockets[0], 'transaction')

      sockets[0].deliver({
        type: 'error',
        id: String(frame?.id),
        error: { code: 'QUERY_ERROR', message: 'UNIQUE constraint failed: ledger.id' },
      })

      await expect(pending).rejects.toThrow('UNIQUE constraint failed: ledger.id')
      transport.close()
    } finally {
      restore()
    }
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
