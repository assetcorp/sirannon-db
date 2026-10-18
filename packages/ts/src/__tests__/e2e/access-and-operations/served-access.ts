import { join } from 'node:path'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions } from '../../../core/types.js'
import type { SirannonServer } from '../../../server/server.js'
import { listenOnLoopback } from '../lib/listening-server.js'
import { connectRaw, readSqlite, releaseServedRegistries, type ServerMessage, within } from '../lib/served-registry.js'

export interface TypedServer<I> {
  readonly server: SirannonServer<I>
  readonly port: number
  readonly base: string
  readonly wsBase: string
}

export interface Order {
  id: number
  customer: string
  total: number
  tenant: string
}

export const ORDERS_SCHEMA =
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL CHECK (total >= 0), tenant TEXT NOT NULL)'
export const SEEDED_ORDERS: readonly Order[] = [
  { id: 1, customer: 'ama', total: 120, tenant: 'acme' },
  { id: 2, customer: 'kofi', total: 80, tenant: 'globex' },
  { id: 3, customer: 'esi', total: 45, tenant: 'acme' },
]

type HeaderWebSocket = new (url: string, init: { headers: Record<string, string>; protocols?: string[] }) => WebSocket

const WebSocketWithHeaders = WebSocket as unknown as HeaderWebSocket

const closers: Array<() => Promise<void>> = []

export async function serveTyped<I>(sirannon: Sirannon, options: ServerOptions<I>): Promise<TypedServer<I>> {
  const server = await listenOnLoopback<I>(sirannon, options)
  closers.push(() => server.close().catch(() => {}))
  const port = server.listeningPort
  return { server, port, base: `http://127.0.0.1:${port}`, wsBase: `ws://127.0.0.1:${port}` }
}

export async function releaseAccessServers(): Promise<void> {
  for (const close of closers.splice(0).reverse()) await close()
  await releaseServedRegistries()
}

export async function openOrders(
  sirannon: Sirannon,
  dir: string,
  id = 'orders',
): Promise<{ db: Database; path: string }> {
  const path = join(dir, `${id}.db`)
  const db = await sirannon.open(id, path)
  await db.execute(ORDERS_SCHEMA)
  for (const order of SEEDED_ORDERS) {
    await db.execute('INSERT INTO orders (id, customer, total, tenant) VALUES (?, ?, ?, ?)', [
      order.id,
      order.customer,
      order.total,
      order.tenant,
    ])
  }
  return { db, path }
}

export function storedOrders(path: string): Order[] {
  return readSqlite<{ id: bigint; customer: string; total: bigint; tenant: string }>(
    path,
    'SELECT id, customer, total, tenant FROM orders ORDER BY id',
  ).map(row => ({ id: Number(row.id), customer: row.customer, total: Number(row.total), tenant: row.tenant }))
}

export interface UpgradeAnswer {
  statusLine: string
  headers: Map<string, string>
}

export function upgradeRequestText(path: string, headers: Record<string, string>): string {
  const lines = [
    `GET ${path} HTTP/1.1`,
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    'Sec-WebSocket-Version: 13',
    'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==',
  ]
  for (const [name, value] of Object.entries(headers)) lines.push(`${name}: ${value}`)
  return `${lines.join('\r\n')}\r\n\r\n`
}

export async function rawUpgrade(port: number, path: string, headers: Record<string, string>): Promise<UpgradeAnswer> {
  const connection = await connectRaw(port)
  connection.socket.write(upgradeRequestText(path, headers))
  await within(
    (async () => {
      while (!connection.received().includes('\r\n\r\n') && !connection.isClosed()) {
        await new Promise(resolve => setTimeout(resolve, 5))
      }
    })(),
    'the upgrade answer',
  )
  const [head = ''] = connection.received().split('\r\n\r\n')
  const [statusLine = '', ...headerLines] = head.split('\r\n')
  const parsed = new Map<string, string>()
  for (const line of headerLines) {
    const colon = line.indexOf(':')
    if (colon > 0) parsed.set(line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim())
  }
  connection.socket.destroy()
  return { statusLine, headers: parsed }
}

export interface RawHttpAnswer {
  status: number
  body: unknown
  closed: boolean
}

export async function rawHttp(port: number, request: string): Promise<RawHttpAnswer> {
  const connection = await connectRaw(port)
  connection.socket.write(request)
  const complete = (): boolean => {
    const text = connection.received()
    const end = text.indexOf('\r\n\r\n')
    if (end < 0) return false
    const length = /content-length: *(\d+)/i.exec(text.slice(0, end))
    return length !== null && Buffer.byteLength(text.slice(end + 4), 'latin1') >= Number(length[1])
  }
  await within(
    (async () => {
      while (!complete() && !connection.isClosed()) await new Promise(resolve => setTimeout(resolve, 5))
    })(),
    'the raw HTTP answer',
  )
  const text = connection.received()
  const closed = connection.isClosed()
  connection.socket.destroy()
  const status = Number(/^HTTP\/1\.1 (\d{3})/.exec(text)?.[1] ?? 0)
  const rawBody = Buffer.from(text.slice(text.indexOf('\r\n\r\n') + 4), 'latin1').toString('utf8')
  let body: unknown = rawBody
  try {
    body = JSON.parse(rawBody)
  } catch {}
  return { status, body, closed }
}

export interface SocketWithHeaders {
  request(message: Record<string, unknown> & { id: string }): Promise<ServerMessage>
  sendText(text: string): Promise<ServerMessage>
  close(): void
}

export async function openSocketWithHeaders(url: string, headers: Record<string, string>): Promise<SocketWithHeaders> {
  const ws = new WebSocketWithHeaders(url, { headers })
  const waiting: Array<(message: ServerMessage) => void> = []
  const replies = new Map<string, (message: ServerMessage) => void>()
  ws.addEventListener('message', event => {
    const message = JSON.parse(String(event.data)) as ServerMessage
    const byId = message.id === undefined ? undefined : replies.get(message.id)
    if (byId && message.id !== undefined) {
      replies.delete(message.id)
      byId(message)
      return
    }
    waiting.shift()?.(message)
  })
  closers.push(async () => ws.close())
  await within(
    new Promise<void>((resolve, reject) => {
      ws.addEventListener('open', () => resolve(), { once: true })
      ws.addEventListener('error', () => reject(new Error(`The WebSocket to ${url} failed to open`)), { once: true })
    }),
    'the WebSocket opening',
  )
  return {
    request: message => {
      const answered = new Promise<ServerMessage>(resolve => replies.set(message.id, resolve))
      ws.send(JSON.stringify(message))
      return within(answered, `the reply to ${message.id}`)
    },
    sendText: text => {
      const answered = new Promise<ServerMessage>(resolve => waiting.push(resolve))
      ws.send(text)
      return within(answered, 'the reply to a raw frame')
    },
    close: () => ws.close(),
  }
}

export interface Gate {
  readonly promise: Promise<void>
  open(): void
}

export function gate(): Gate {
  let open: () => void = () => {}
  const promise = new Promise<void>(resolve => {
    open = resolve
  })
  return { promise, open }
}

export function closeOf(ws: WebSocket): Promise<{ code: number; reason: string }> {
  return new Promise(resolve => {
    ws.addEventListener('error', () => {})
    ws.addEventListener('close', event => resolve({ code: event.code, reason: event.reason }))
  })
}
