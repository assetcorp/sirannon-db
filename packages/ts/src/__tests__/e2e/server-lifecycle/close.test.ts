import { mkdtempSync, rmSync } from 'node:fs'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { Params, QueryOptions, ServerExecutionTarget, ServerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../../server/server.js'
import { listenOnLoopback } from '../lib/listening-server.js'
import { RawDeviceClient } from '../lib/raw-device-client.js'
import { readSqlite } from '../lib/served-registry.js'

const DATABASE_ID = 'council'
const COUNT_SQL = 'SELECT count(*) AS n FROM minutes'
const COUNT_ROWS = JSON.stringify({ sql: COUNT_SQL })
const SOCKET_DEADLINE_MS = 3_000
const DECLARED_BODY_BYTES = 1_000
const WS_CLOSE_GOING_AWAY = 1001
const LARGE_REPLY_ROWS = 4_000
const LARGE_REPLY_HEX_BYTES = 500
const QUEUED_REPLIES = 3
const REPLIES_QUEUED_MS = 400
const READER_PAUSED_WHILE_CLOSING_MS = 300
const CLOSE_TIMEOUT_MS = 250
const TIMER_SLACK_MS = 25
const LARGEST_TIMER_DELAY_MS = 2_147_483_647
const SELECT_ALL_MINUTES = 'SELECT id, body FROM minutes ORDER BY id'

interface Served {
  sirannon: Sirannon
  server: SirannonServer
  port: number
  path: string
}

interface HeldReads {
  options: (sirannon: Sirannon) => ServerOptions
  entered: () => number
  release: () => void
}

interface RawConnection {
  socket: net.Socket
  received(): string
  closed: Promise<void>
  isClosed(): boolean
}

let dir: string
const running: Served[] = []
const rawClients: RawDeviceClient[] = []

async function serve(
  rows: number,
  port = 0,
  options?: ServerOptions | ((sirannon: Sirannon) => ServerOptions),
): Promise<Served> {
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  const path = join(mkdtempSync(join(dir, 'node-')), 'council.db')
  const db = await sirannon.open(DATABASE_ID, path)
  await db.execute('CREATE TABLE minutes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  for (let index = 0; index < rows; index++) {
    await db.execute('INSERT INTO minutes (body) VALUES (?)', [`item ${index}`])
  }
  const resolved = typeof options === 'function' ? options(sirannon) : options
  const server = await listenOnLoopback(sirannon, { ...resolved, acceptSql: true, port })
  const served = { sirannon, server, port: server.listeningPort, path }
  running.push(served)
  return served
}

function forget(served: Served): void {
  const index = running.indexOf(served)
  if (index !== -1) running.splice(index, 1)
}

function heldReads(): HeldReads {
  let entered = 0
  let release: () => void = () => {}
  const gate = new Promise<void>(resolve => {
    release = resolve
  })
  return {
    options: sirannon => ({
      resolveExecutionTarget: async id => {
        const db = await sirannon.resolve(id)
        if (!db) return undefined
        const target: ServerExecutionTarget = {
          query: async <T>(sql: string, params?: Params, queryOptions?: QueryOptions): Promise<T[]> => {
            entered++
            await gate
            return db.query<T>(sql, params, queryOptions)
          },
          execute: (sql, params, executeOptions) => db.execute(sql, params, executeOptions),
          transaction: fn => db.transaction(fn),
        }
        return target
      },
    }),
    entered: () => entered,
    release: () => release(),
  }
}

async function rawClient(port: number): Promise<RawDeviceClient> {
  const client = await RawDeviceClient.connect(port, DATABASE_ID)
  rawClients.push(client)
  return client
}

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function stop(served: Served): Promise<void> {
  forget(served)
  await served.server.close()
  await served.sirannon.shutdown()
}

function postRequest(body: string, declaredLength = Buffer.byteLength(body)): string {
  return [
    `POST /db/${DATABASE_ID}/query HTTP/1.1`,
    'Host: 127.0.0.1',
    'Content-Type: application/json',
    `Content-Length: ${declaredLength}`,
    '',
    body,
  ].join('\r\n')
}

async function connect(port: number): Promise<RawConnection> {
  const socket = net.connect(port, '127.0.0.1')
  let text = ''
  let closed = false
  socket.on('data', chunk => {
    text += chunk.toString()
  })
  socket.on('error', () => {})
  const closing = new Promise<void>(resolve => {
    socket.on('close', () => {
      closed = true
      resolve()
    })
  })
  await new Promise<void>((resolve, reject) => {
    socket.once('connect', resolve)
    socket.once('error', reject)
  })
  return { socket, received: () => text, closed: closing, isClosed: () => closed }
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} took longer than ${SOCKET_DEADLINE_MS} ms`)),
      SOCKET_DEADLINE_MS,
    )
  })
  try {
    return await Promise.race([promise, deadline])
  } finally {
    clearTimeout(timer)
  }
}

async function waitFor(condition: () => boolean, label: string): Promise<void> {
  await within(
    (async () => {
      while (!condition()) await new Promise(resolve => setTimeout(resolve, 5))
    })(),
    label,
  )
}

function rowCountIn(response: string): number | null {
  const match = response.match(/"n":(\d+)/)
  return match ? Number(match[1]) : null
}

function heldAuthentication(): { options: ServerOptions; entered: () => boolean; release: () => void } {
  let entered = false
  let release: () => void = () => {}
  const gate = new Promise<void>(resolve => {
    release = resolve
  })
  return {
    options: {
      authenticate: async () => {
        entered = true
        await gate
        return { user: 'clerk' }
      },
    },
    entered: () => entered,
    release: () => release(),
  }
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-server-close-'))
})

afterEach(async () => {
  for (const client of rawClients.splice(0)) client.destroy()
  for (const served of running.splice(0).reverse()) {
    await served.server.close().catch(() => {})
    await served.sirannon.shutdown().catch(() => {})
  }
  rmSync(dir, { recursive: true, force: true })
})

describe('closing a server', () => {
  it('ends idle keep-alive connections, so the next request reaches the server that listens next on the same port', async () => {
    const first = await serve(0)
    const kept = await connect(first.port)
    kept.socket.write(postRequest(COUNT_ROWS))
    await waitFor(() => kept.received().includes('\r\n\r\n{'), 'the first response')
    expect(kept.received()).toMatch(/^HTTP\/1\.1 200/)
    expect(rowCountIn(kept.received())).toBe(0)

    const pooled = await fetch(`http://127.0.0.1:${first.port}/db/${DATABASE_ID}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: COUNT_ROWS,
    })
    expect(await pooled.json()).toEqual({ rows: [{ n: 0 }] })

    await within(stop(first), 'closing the first server')
    await within(kept.closed, 'the idle connection closing')

    const second = await serve(3, first.port)
    const answered = await fetch(`http://127.0.0.1:${second.port}/db/${DATABASE_ID}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: COUNT_ROWS,
    })

    expect(answered.status).toBe(200)
    expect(await answered.json()).toEqual({ rows: [{ n: 3 }] })
  })

  it('lets a request that is in progress finish before close resolves, and closes a connection that sends a request meanwhile', async () => {
    const held = heldAuthentication()
    const served = await serve(2, 0, held.options)
    const working = await connect(served.port)
    const idle = await connect(served.port)
    working.socket.write(postRequest(COUNT_ROWS))
    await waitFor(held.entered, 'the request reaching the authenticate hook')

    let closeResolved = false
    const closing = served.server.close().then(() => {
      closeResolved = true
    })
    idle.socket.write(postRequest(COUNT_ROWS))
    await within(idle.closed, 'the connection that sent a request after closing began')

    expect(idle.received()).toBe('')
    expect(closeResolved).toBe(false)
    expect(working.isClosed()).toBe(false)

    held.release()
    await within(closing, 'close after the request finished')
    await within(working.closed, 'the finished connection closing')

    expect(working.received()).toMatch(/^HTTP\/1\.1 200/)
    expect(rowCountIn(working.received())).toBe(2)
  })

  it('cuts a client that is still sending its body, so a stalled upload cannot hold close open', async () => {
    const served = await serve(1)
    const stalled = await connect(served.port)
    stalled.socket.write(postRequest('{"sql":', DECLARED_BODY_BYTES))
    await new Promise(resolve => setTimeout(resolve, 50))

    await within(served.server.close(), 'close with an upload stalled')
    await within(stalled.closed, 'the stalled upload losing its connection')

    expect(stalled.received()).toBe('')
  })

  it('sends a WebSocket client the going-away close code', async () => {
    const served = await serve(1)
    const ws = new WebSocket(`ws://127.0.0.1:${served.port}/db/${DATABASE_ID}`)
    await within(
      new Promise<void>((resolve, reject) => {
        ws.addEventListener('open', () => resolve())
        ws.addEventListener('error', () => reject(new Error('The WebSocket failed to open')))
      }),
      'the WebSocket opening',
    )
    const closedWith = new Promise<number>(resolve => {
      ws.addEventListener('close', event => resolve(event.code))
    })

    await within(served.server.close(), 'close with a WebSocket open')

    expect(await within(closedWith, 'the WebSocket closing')).toBe(1001)
  })

  it('delivers every reply that a slow reader has queued and then the going-away close code, and resolves only once that reader has read them', async () => {
    const served = await serve(0)
    const db = served.sirannon.get(DATABASE_ID)
    await db?.execute(
      `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${LARGE_REPLY_ROWS}) INSERT INTO minutes (body) SELECT hex(randomblob(${LARGE_REPLY_HEX_BYTES})) FROM n`,
    )
    const reader = await rawClient(served.port)
    reader.pauseReading()
    for (let reply = 0; reply < QUEUED_REPLIES; reply++) {
      reader.send({ type: 'query', id: `minutes-${reply}`, sql: SELECT_ALL_MINUTES })
    }
    await pause(REPLIES_QUEUED_MS)

    let closeResolved = false
    const closing = served.server.close().then(() => {
      closeResolved = true
    })
    await pause(READER_PAUSED_WHILE_CLOSING_MS)
    const resolvedWhilePaused = closeResolved
    reader.resumeReading()
    await within(reader.waitForClose(), 'the slow reader receiving the close frame')
    await within(closing, 'close after the slow reader read its replies')

    const expected = readSqlite<{ id: bigint; body: string }>(served.path, SELECT_ALL_MINUTES).map(row => ({
      id: Number(row.id),
      body: row.body,
    }))
    expect(expected).toHaveLength(LARGE_REPLY_ROWS)
    expect(resolvedWhilePaused).toBe(false)
    expect(reader.messages.map(message => message.id)).toEqual(
      Array.from({ length: QUEUED_REPLIES }, (_, reply) => `minutes-${reply}`),
    )
    for (const message of reader.messages) {
      expect(message.data, String(message.id)).toEqual({ rows: expected })
    }
    expect(reader.closeCode).toBe(WS_CLOSE_GOING_AWAY)
  })

  it('lets a WebSocket request in progress reply before it closes that connection, and answers a request sent meanwhile with HANDLER_CLOSED', async () => {
    const held = heldReads()
    const served = await serve(2, 0, held.options)
    const reader = await rawClient(served.port)
    reader.send({ type: 'query', id: 'held', sql: COUNT_SQL })
    await waitFor(() => held.entered() === 1, 'the request reaching the execution target')

    let closeResolved = false
    const closing = served.server.close().then(() => {
      closeResolved = true
    })
    const late = await within(reader.request({ type: 'query', id: 'late', sql: COUNT_SQL }), 'the late request')
    const openWhileHeld = !reader.socketClosed && !closeResolved

    held.release()
    const answered = await within(
      reader.waitForMessage(message => message.id === 'held'),
      'the reply to the request in progress',
    )
    await within(reader.waitForClose(), 'the connection closing after the reply')
    await within(closing, 'close after the request replied')

    expect(late).toMatchObject({ type: 'error', id: 'late', error: { code: 'HANDLER_CLOSED' } })
    expect(openWhileHeld).toBe(true)
    expect(answered).toEqual({ type: 'result', id: 'held', data: { rows: [{ n: 2 }] } })
    expect(held.entered()).toBe(1)
    expect(reader.closeCode).toBe(WS_CLOSE_GOING_AWAY)
  })

  it('waits for a WebSocket upgrade whose authenticate hook is in progress, then refuses that connection with HANDLER_CLOSED and the going-away close code', async () => {
    const held = heldAuthentication()
    const served = await serve(1, 0, held.options)
    const upgrading = RawDeviceClient.connect(served.port, DATABASE_ID)
    await waitFor(held.entered, 'the upgrade reaching the authenticate hook')

    let closeResolved = false
    const closing = served.server.close().then(() => {
      closeResolved = true
    })
    await pause(READER_PAUSED_WHILE_CLOSING_MS)
    const resolvedWhileHeld = closeResolved
    held.release()
    const client = await within(upgrading, 'the upgrade completing')
    rawClients.push(client)
    await within(client.waitForClose(), 'the refused connection closing')
    await within(closing, 'close after the upgrade')

    expect(resolvedWhileHeld).toBe(false)
    expect(client.messages).toEqual([
      { type: 'error', id: '', error: { code: 'HANDLER_CLOSED', message: expect.any(String) } },
    ])
    expect(client.closeCode).toBe(WS_CLOSE_GOING_AWAY)
  })

  it('ends every connection once the close deadline passes, so that a hook or a request that never settles cannot hold close open and shutdown runs after it', async () => {
    const held = heldReads()
    const served = await serve(1, 0, sirannon => ({
      ...held.options(sirannon),
      authenticate: ctx => (ctx.headers['x-hold'] === 'forever' ? new Promise<never>(() => {}) : { user: 'clerk' }),
    }))
    forget(served)
    const stuck = await connect(served.port)
    stuck.socket.write(postRequest(COUNT_ROWS).replace('Host: 127.0.0.1', 'Host: 127.0.0.1\r\nX-Hold: forever'))
    const reader = await rawClient(served.port)
    reader.send({ type: 'query', id: 'held', sql: COUNT_SQL })
    await waitFor(() => held.entered() === 1, 'the WebSocket request reaching the execution target')

    const started = performance.now()
    await within(served.server.close({ timeoutMs: CLOSE_TIMEOUT_MS }), 'close with requests that never settle')
    const elapsed = performance.now() - started
    await within(stuck.closed, 'the HTTP connection with a stuck hook closing')
    await within(reader.waitForClose(), 'the WebSocket with a stuck request closing')
    await within(served.sirannon.shutdown(), 'shutdown after close')

    expect(elapsed).toBeGreaterThanOrEqual(CLOSE_TIMEOUT_MS - TIMER_SLACK_MS)
    expect(stuck.received()).toBe('')
    expect(reader.messages.some(message => message.id === 'held')).toBe(false)
    expect(reader.closeCode).toBe(WS_CLOSE_GOING_AWAY)
  })

  it('refuses a close timeout that is not a whole number of milliseconds within the timer range, and keeps serving', async () => {
    const served = await serve(1)
    for (const timeoutMs of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, LARGEST_TIMER_DELAY_MS + 1]) {
      let thrown: unknown
      try {
        await served.server.close({ timeoutMs })
      } catch (err) {
        thrown = err
      }
      expect(thrown instanceof SirannonError ? thrown.code : thrown, String(timeoutMs)).toBe('INVALID_CLOSE_TIMEOUT')
    }

    const answered = await fetch(`http://127.0.0.1:${served.port}/db/${DATABASE_ID}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: COUNT_ROWS,
    })
    expect(await answered.json()).toEqual({ rows: [{ n: 1 }] })
  })
})
