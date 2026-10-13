import { mkdtempSync, rmSync } from 'node:fs'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../../server/server.js'

const DATABASE_ID = 'council'
const COUNT_ROWS = JSON.stringify({ sql: 'SELECT count(*) AS n FROM minutes' })
const SOCKET_DEADLINE_MS = 3_000
const DECLARED_BODY_BYTES = 1_000

interface Served {
  sirannon: Sirannon
  server: SirannonServer
  port: number
}

interface RawConnection {
  socket: net.Socket
  received(): string
  closed: Promise<void>
  isClosed(): boolean
}

let dir: string
const running: Served[] = []

async function serve(rows: number, port = 0, options?: ServerOptions): Promise<Served> {
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  const db = await sirannon.open(DATABASE_ID, join(mkdtempSync(join(dir, 'node-')), 'council.db'))
  await db.execute('CREATE TABLE minutes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  for (let index = 0; index < rows; index++) {
    await db.execute('INSERT INTO minutes (body) VALUES (?)', [`item ${index}`])
  }
  const server = createServer(sirannon, { ...options, acceptSql: true, port })
  await server.listen()
  const served = { sirannon, server, port: server.listeningPort }
  running.push(served)
  return served
}

async function stop(served: Served): Promise<void> {
  const index = running.indexOf(served)
  if (index !== -1) running.splice(index, 1)
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
})
