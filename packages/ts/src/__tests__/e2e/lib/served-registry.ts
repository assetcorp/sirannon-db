import { mkdtempSync, rmSync } from 'node:fs'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../../server/server.js'
import { listenOnLoopback } from './listening-server.js'

const SOCKET_DEADLINE_MS = 5_000
const POLL_INTERVAL_MS = 5

export interface ServedRegistry {
  readonly sirannon: Sirannon
  readonly server: SirannonServer
  readonly port: number
  readonly base: string
  readonly wsBase: string
}

export interface RawAnswer {
  status: number
  headers: Headers
  text: string
  body: unknown
}

export interface ServerMessage {
  type?: string
  id?: string
  data?: Record<string, unknown>
  error?: { code?: string; message?: string }
  [key: string]: unknown
}

const cleanups: Array<() => Promise<void> | void> = []

export function scratchDirectory(label: string): string {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-${label}-`))
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }))
  return dir
}

export function trackedRegistry(options?: Partial<SirannonOptions>): Sirannon {
  const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
  cleanups.push(() => sirannon.shutdown().catch(() => {}))
  return sirannon
}

export async function serveRegistry(sirannon: Sirannon, options?: ServerOptions): Promise<ServedRegistry> {
  const server = await listenOnLoopback(sirannon, options ?? {})
  cleanups.push(() => server.close().catch(() => {}))
  const port = server.listeningPort
  return {
    sirannon,
    server,
    port,
    base: `http://127.0.0.1:${port}`,
    wsBase: `ws://127.0.0.1:${port}`,
  }
}

export async function releaseServedRegistries(): Promise<void> {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
}

function parsedOrText(text: string): unknown {
  if (text.length === 0) return undefined
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

export async function sendRaw(
  base: string,
  method: string,
  path: string,
  body?: string,
  headers: Record<string, string> = {},
): Promise<RawAnswer> {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: body === undefined ? headers : { 'content-type': 'application/json', ...headers },
    body,
  })
  const text = await response.text()
  return { status: response.status, headers: response.headers, text, body: parsedOrText(text) }
}

export function postJson(base: string, path: string, value: unknown): Promise<RawAnswer> {
  return sendRaw(base, 'POST', path, JSON.stringify(value))
}

export function errorCodeOf(body: unknown): string | null {
  return (body as { error?: { code?: string } } | undefined)?.error?.code ?? null
}

export function errorMessageOf(body: unknown): string {
  return (body as { error?: { message?: string } } | undefined)?.error?.message ?? ''
}

export async function within<T>(promise: Promise<T>, label: string, deadlineMs = SOCKET_DEADLINE_MS): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} took longer than ${deadlineMs} ms`)), deadlineMs)
  })
  try {
    return await Promise.race([promise, deadline])
  } finally {
    clearTimeout(timer)
  }
}

export async function waitUntil(condition: () => boolean, label: string): Promise<void> {
  await within(
    (async () => {
      while (!condition()) await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS))
    })(),
    label,
  )
}

export function readSqlite<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    raw.defaultSafeIntegers(true)
    return raw.prepare(sql).all(...params) as T[]
  } finally {
    raw.close()
  }
}

export function countSqlite(path: string, table: string): number {
  const [row] = readSqlite<{ n: bigint }>(path, `SELECT count(*) AS n FROM "${table}"`)
  return Number(row?.n ?? -1n)
}

export class RawSocket {
  private readonly received: ServerMessage[] = []
  private readonly waiters: Array<() => void> = []
  readonly closed: Promise<{ code: number; reason: string }>

  private constructor(private readonly ws: WebSocket) {
    ws.addEventListener('message', event => {
      this.received.push(parsedOrText(String(event.data)) as ServerMessage)
      for (const wake of this.waiters.splice(0)) wake()
    })
    this.closed = new Promise(resolve => {
      ws.addEventListener('close', event => {
        resolve({ code: event.code, reason: event.reason })
        for (const wake of this.waiters.splice(0)) wake()
      })
    })
  }

  static async open(url: string): Promise<RawSocket> {
    const ws = new WebSocket(url)
    const socket = new RawSocket(ws)
    cleanups.push(() => socket.close())
    await within(
      new Promise<void>((resolve, reject) => {
        ws.addEventListener('open', () => resolve(), { once: true })
        ws.addEventListener('error', () => reject(new Error(`The WebSocket to ${url} failed to open`)), { once: true })
      }),
      'the WebSocket opening',
    )
    return socket
  }

  get messages(): readonly ServerMessage[] {
    return this.received
  }

  get open(): boolean {
    return this.ws.readyState === WebSocket.OPEN
  }

  sendText(text: string): void {
    this.ws.send(text)
  }

  async next(): Promise<ServerMessage> {
    const seen = this.received.length
    await this.until(() => this.received.length > seen, 'the next WebSocket message')
    return this.received[seen] as ServerMessage
  }

  async reply(id: string): Promise<ServerMessage> {
    const already = this.received.length
    await this.until(() => this.received.slice(already).some(message => message.id === id), `the reply to ${id}`)
    return this.received.slice(already).find(message => message.id === id) as ServerMessage
  }

  async request(message: Record<string, unknown> & { id: string }): Promise<ServerMessage> {
    const answered = this.reply(message.id)
    this.ws.send(JSON.stringify(message))
    return answered
  }

  close(): void {
    if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) this.ws.close()
  }

  private async until(condition: () => boolean, label: string): Promise<void> {
    await within(
      (async () => {
        while (!condition()) {
          if (this.ws.readyState === WebSocket.CLOSED)
            throw new Error(`The WebSocket closed while waiting for ${label}`)
          await new Promise<void>(resolve => {
            this.waiters.push(resolve)
            setTimeout(resolve, POLL_INTERVAL_MS * 20)
          })
        }
      })(),
      label,
    )
  }
}

export interface RawConnection {
  readonly socket: net.Socket
  received(): string
  readonly closed: Promise<void>
  isClosed(): boolean
}

export async function connectRaw(port: number): Promise<RawConnection> {
  const socket = net.connect(port, '127.0.0.1')
  let text = ''
  let closed = false
  socket.on('data', chunk => {
    text += chunk.toString('latin1')
  })
  socket.on('error', () => {})
  const closing = new Promise<void>(resolve => {
    socket.on('close', () => {
      closed = true
      resolve()
    })
  })
  cleanups.push(() => {
    socket.destroy()
  })
  await within(
    new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve)
      socket.once('error', reject)
    }),
    'the raw connection opening',
  )
  return { socket, received: () => text, closed: closing, isClosed: () => closed }
}

export function httpRequestText(
  method: string,
  path: string,
  body: string,
  headers: Record<string, string | number> = {},
): string {
  const lines = [`${method} ${path} HTTP/1.1`, 'Host: 127.0.0.1', 'Content-Type: application/json']
  const declared = { 'Content-Length': Buffer.byteLength(body), ...headers }
  for (const [name, value] of Object.entries(declared)) lines.push(`${name}: ${value}`)
  return `${lines.join('\r\n')}\r\n\r\n${body}`
}

export function statusLinesIn(text: string): number[] {
  return [...text.matchAll(/HTTP\/1\.1 (\d{3})/g)].map(match => Number(match[1]))
}
