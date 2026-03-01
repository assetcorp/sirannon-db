import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { createServer, type SirannonServer } from '../server.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let wsUrl: string

function sendAndReceive(ws: WebSocket, msg: Record<string, unknown>): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for WS response')), 3000)
    const handler = (event: MessageEvent) => {
      clearTimeout(timeout)
      ws.removeEventListener('message', handler)
      resolve(JSON.parse(String(event.data)))
    }
    ws.addEventListener('message', handler)
    ws.send(JSON.stringify(msg))
  })
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws.readyState === WebSocket.OPEN) {
      resolve()
      return
    }
    const timeout = setTimeout(() => reject(new Error('WebSocket open timed out')), 3000)
    ws.addEventListener(
      'open',
      () => {
        clearTimeout(timeout)
        resolve()
      },
      { once: true },
    )
    ws.addEventListener(
      'error',
      () => {
        clearTimeout(timeout)
        reject(new Error('WebSocket connect error'))
      },
      { once: true },
    )
  })
}

function waitForClose(ws: WebSocket): Promise<CloseEvent> {
  return new Promise((resolve, reject) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolve(new CloseEvent('close'))
      return
    }
    const timeout = setTimeout(() => reject(new Error('WebSocket close timed out')), 3000)
    ws.addEventListener(
      'close',
      e => {
        clearTimeout(timeout)
        resolve(e)
      },
      { once: true },
    )
  })
}

function collectMessages(ws: WebSocket, count: number, timeoutMs = 3000): Promise<Record<string, unknown>[]> {
  return new Promise(resolve => {
    const messages: Record<string, unknown>[] = []
    const timeout = setTimeout(() => {
      ws.removeEventListener('message', handler)
      resolve(messages)
    }, timeoutMs)
    const handler = (event: MessageEvent) => {
      messages.push(JSON.parse(String(event.data)))
      if (messages.length >= count) {
        clearTimeout(timeout)
        ws.removeEventListener('message', handler)
        resolve(messages)
      }
    }
    ws.addEventListener('message', handler)
  })
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-e2e-'))
  sirannon = new Sirannon()
  const db = sirannon.open('test', join(tempDir, 'test.db'))
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  wsUrl = `ws://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('E2E WebSocket', () => {
  it('queries data over WebSocket', async () => {
    const ws = new WebSocket(`${wsUrl}/db/test`)
    await waitForOpen(ws)

    const result = await sendAndReceive(ws, {
      id: 'q1',
      type: 'query',
      sql: 'SELECT * FROM users ORDER BY id',
    })

    expect(result.type).toBe('result')
    expect(result.id).toBe('q1')
    const data = result.data as Record<string, unknown>
    const rows = data.rows as Record<string, unknown>[]
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('Alice')
    expect(rows[1].name).toBe('Bob')

    ws.close()
  })

  it('executes mutations over WebSocket', async () => {
    const ws = new WebSocket(`${wsUrl}/db/test`)
    await waitForOpen(ws)

    const result = await sendAndReceive(ws, {
      id: 'e1',
      type: 'execute',
      sql: "INSERT INTO users (name, age) VALUES ('Carol', 28)",
    })

    expect(result.type).toBe('result')
    const data = result.data as Record<string, unknown>
    expect(data.changes).toBe(1)
    expect(data.lastInsertRowId).toBeDefined()

    const verify = await sendAndReceive(ws, {
      id: 'q1',
      type: 'query',
      sql: 'SELECT COUNT(*) as count FROM users',
    })
    const verifyData = verify.data as Record<string, unknown>
    expect((verifyData.rows as Record<string, unknown>[])[0].count).toBe(3)

    ws.close()
  })

  it('subscribes to CDC changes and receives events', async () => {
    const ws = new WebSocket(`${wsUrl}/db/test`)
    await waitForOpen(ws)

    const subResult = await sendAndReceive(ws, {
      id: 'sub-1',
      type: 'subscribe',
      table: 'users',
    })
    expect(subResult.type).toBe('subscribed')

    const changePromise = collectMessages(ws, 1)

    const db = sirannon.get('test')
    db?.execute("INSERT INTO users (name, age) VALUES ('Dave', 40)")

    const changes = await changePromise
    expect(changes.length).toBeGreaterThanOrEqual(1)
    const changeMsg = changes.find(m => m.type === 'change')
    expect(changeMsg).toBeDefined()
    expect(changeMsg?.id).toBe('sub-1')
    const event = changeMsg?.event as Record<string, unknown>
    expect(event.type).toBe('insert')
    expect(event.table).toBe('users')
    expect((event.row as Record<string, unknown>).name).toBe('Dave')

    ws.close()
  })

  it('unsubscribes and stops receiving events', async () => {
    const ws = new WebSocket(`${wsUrl}/db/test`)
    await waitForOpen(ws)

    await sendAndReceive(ws, { id: 'sub-1', type: 'subscribe', table: 'users' })

    const unsubResult = await sendAndReceive(ws, { id: 'sub-1', type: 'unsubscribe' })
    expect(unsubResult.type).toBe('unsubscribed')

    sirannon.get('test')?.execute("INSERT INTO users (name, age) VALUES ('Eve', 22)")

    const collected = await collectMessages(ws, 1, 300)
    const changeMessages = collected.filter(m => m.type === 'change')
    expect(changeMessages).toHaveLength(0)

    ws.close()
  })

  it('rejects WS upgrade when onRequest denies', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: () => ({ status: 403, code: 'FORBIDDEN', message: 'No WS for you' }),
    })
    await hookServer.listen()
    const hookWsUrl = `ws://127.0.0.1:${hookServer.listeningPort}`

    try {
      const ws = new WebSocket(`${hookWsUrl}/db/test`)
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Timed out')), 3000)
        ws.addEventListener(
          'error',
          () => {
            clearTimeout(timeout)
            resolve()
          },
          { once: true },
        )
        ws.addEventListener(
          'close',
          () => {
            clearTimeout(timeout)
            resolve()
          },
          { once: true },
        )
      })
      expect(ws.readyState).not.toBe(WebSocket.OPEN)
    } finally {
      await hookServer.close()
    }
  })

  it('allows WS upgrade when onRequest approves', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: ({ headers }) => {
        if (headers['x-token'] !== 'valid') {
          return { status: 401, code: 'UNAUTHORIZED', message: 'Bad token' }
        }
      },
    })
    await hookServer.listen()
    const hookWsUrl = `ws://127.0.0.1:${hookServer.listeningPort}`

    try {
      const ws = new WebSocket(`${hookWsUrl}/db/test`, { headers: { 'x-token': 'valid' } } as never)
      await waitForOpen(ws)

      const result = await sendAndReceive(ws, { id: 'q1', type: 'query', sql: 'SELECT 1 as val' })
      expect(result.type).toBe('result')

      ws.close()
    } finally {
      await hookServer.close()
    }
  })

  it('receives error for unknown database on WS connect', async () => {
    const ws = new WebSocket(`${wsUrl}/db/nonexistent`)
    await waitForOpen(ws)

    const closePromise = waitForClose(ws)
    const msgPromise = collectMessages(ws, 1)

    const messages = await msgPromise
    const errorMsg = messages.find(m => m.type === 'error')
    expect(errorMsg).toBeDefined()
    expect((errorMsg?.error as Record<string, unknown>).code).toBe('DATABASE_NOT_FOUND')

    await closePromise
  })
})
