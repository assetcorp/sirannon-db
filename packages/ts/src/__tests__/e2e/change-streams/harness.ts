import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, ServerOptions, SirannonOptions } from '../../../core/types.js'
import { listenOnLoopback } from '../lib/listening-server.js'
import {
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  trackedRegistry,
} from '../lib/served-registry.js'

export const QUIET_PERIOD_MS = 250
const WAIT_STEP_MS = 5
const DEFAULT_DEADLINE_MS = 10_000
const REPLY_TYPES = ['subscribed', 'unsubscribed', 'error', 'result']

export interface WireEvent {
  type: string
  table: string
  row: Record<string, unknown>
  oldRow?: Record<string, unknown>
  seq: string
  timestamp: number
  rowId?: string
  txId?: string
  txEnd?: boolean
  hlc?: string
  origin?: string
}

export interface WireOp {
  op: 'insert' | 'update' | 'delete'
  index: number
  row?: Record<string, unknown>
}

export interface WireMessage {
  type?: string
  id?: string
  event?: WireEvent
  events?: WireEvent[]
  error?: { code?: string; message?: string }
  data?: Record<string, unknown>
  rows?: Record<string, unknown>[]
  ops?: WireOp[]
  revalidating?: boolean
  seq?: string
  epoch?: string
  resync?: boolean
  [key: string]: unknown
}

export interface ChangeLogRow {
  seq: bigint
  table_name: string
  operation: string
  row_id: string
  tx_id: string
  node_id: string
  hlc: string
  new_data: string | null
  old_data: string | null
}

export interface ServedDatabase {
  served: ServedServer
  sirannon: Sirannon
  db: Database
  path: string
  dir: string
}

export interface ServedDatabaseOptions<Identity> {
  label: string
  databaseId: string
  schema: readonly string[]
  registry?: Partial<SirannonOptions>
  database?: DatabaseOptions
  server?: ServerOptions<Identity>
}

const openSockets: WireSocket[] = []
const openServers: Array<{ close(): Promise<void> }> = []

export interface ServedServer extends Omit<ServedRegistry, 'server'> {
  readonly server: { close(): Promise<void> }
}

export async function serveWithIdentity<Identity>(
  sirannon: Sirannon,
  options: ServerOptions<Identity>,
): Promise<ServedServer> {
  const server = await listenOnLoopback<Identity>(sirannon, options)
  openServers.push(server)
  const port = server.listeningPort
  return { sirannon, server, port, base: `http://127.0.0.1:${port}`, wsBase: `ws://127.0.0.1:${port}` }
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitFor(
  condition: () => boolean,
  label: string,
  deadlineMs = DEFAULT_DEADLINE_MS,
): Promise<void> {
  const deadline = Date.now() + deadlineMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Waited longer than ${deadlineMs} ms for ${label}`)
    await pause(WAIT_STEP_MS)
  }
}

export class WireSocket {
  readonly messages: WireMessage[] = []
  readonly closed: Promise<{ code: number; reason: string }>

  private constructor(private readonly ws: WebSocket) {
    ws.addEventListener('message', event => {
      this.messages.push(JSON.parse(String(event.data)) as WireMessage)
    })
    this.closed = new Promise(resolve => {
      ws.addEventListener('close', event => resolve({ code: event.code, reason: event.reason }))
    })
  }

  static async open(url: string, headers?: Record<string, string>): Promise<WireSocket> {
    const HeaderedWebSocket = WebSocket as unknown as new (
      url: string,
      init: { headers: Record<string, string> },
    ) => WebSocket
    const ws = headers === undefined ? new WebSocket(url) : new HeaderedWebSocket(url, { headers })
    const socket = new WireSocket(ws)
    openSockets.push(socket)
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener('open', () => resolve(), { once: true })
      ws.addEventListener('error', () => reject(new Error(`The WebSocket to ${url} failed to open`)), { once: true })
    })
    return socket
  }

  get isOpen(): boolean {
    return this.ws.readyState === WebSocket.OPEN
  }

  send(message: Record<string, unknown>): void {
    this.ws.send(JSON.stringify(message))
  }

  sendText(text: string): void {
    this.ws.send(text)
  }

  async request(message: Record<string, unknown> & { id: string }, types = REPLY_TYPES): Promise<WireMessage> {
    const from = this.messages.length
    this.send(message)
    let reply: WireMessage | undefined
    await waitFor(() => {
      reply = this.messages
        .slice(from)
        .find(candidate => candidate.id === message.id && types.includes(candidate.type ?? ''))
      return reply !== undefined
    }, `the reply to ${message.id}`)
    return reply as WireMessage
  }

  of(id: string, type?: string): WireMessage[] {
    return this.messages.filter(message => message.id === id && (type === undefined || message.type === type))
  }

  events(id: string): WireEvent[] {
    const events: WireEvent[] = []
    for (const message of this.of(id)) {
      if (message.type === 'change' && message.event !== undefined) events.push(message.event)
      if (message.type === 'changes' && message.events !== undefined) events.push(...message.events)
    }
    return events
  }

  async waitForEvents(id: string, count: number, deadlineMs = DEFAULT_DEADLINE_MS): Promise<WireEvent[]> {
    await waitFor(() => this.events(id).length >= count, `${count} change events on ${id}`, deadlineMs)
    await pause(QUIET_PERIOD_MS)
    return this.events(id)
  }

  close(): void {
    if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) this.ws.close()
  }
}

export async function releaseChangeStreams(): Promise<void> {
  for (const socket of openSockets.splice(0)) socket.close()
  for (const server of openServers.splice(0).reverse()) await server.close().catch(() => {})
  await releaseServedRegistries()
}

export async function serveDatabase<Identity = unknown>(
  options: ServedDatabaseOptions<Identity>,
): Promise<ServedDatabase> {
  const dir = scratchDirectory(options.label)
  const path = join(dir, `${options.databaseId}.db`)
  const sirannon = trackedRegistry(options.registry)
  const db = await sirannon.open(options.databaseId, path, options.database)
  for (const statement of options.schema) await db.execute(statement)
  const served = await serveWithIdentity<Identity>(sirannon, { acceptSql: true, ...options.server })
  return { served, sirannon, db, path, dir }
}

export function readFile<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  return readSqlite<T>(path, sql, params)
}

export function writeFile(path: string, sql: string, params: unknown[] = []): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    raw.defaultSafeIntegers(true)
    raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

export function changeLog(path: string, table?: string): ChangeLogRow[] {
  const columns = 'seq, table_name, operation, row_id, tx_id, node_id, hlc, new_data, old_data'
  const sql =
    table === undefined
      ? `SELECT ${columns} FROM _sirannon_changes ORDER BY seq`
      : `SELECT ${columns} FROM _sirannon_changes WHERE table_name = ? ORDER BY seq`
  return readFile<ChangeLogRow>(path, sql, table === undefined ? [] : [table])
}

export function highestSeq(path: string): bigint {
  const [row] = readFile<{ seq: bigint | null }>(path, 'SELECT max(seq) AS seq FROM _sirannon_changes')
  return row?.seq ?? 0n
}

export function cdcEpochOf(path: string): string {
  const [row] = readFile<{ value: string }>(path, "SELECT value FROM _sirannon_meta WHERE key = 'cdc_epoch'")
  return row?.value ?? ''
}

export function triggerNames(path: string): string[] {
  return readFile<{ name: string }>(path, "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name").map(
    row => row.name,
  )
}

export function stampsFromLog(row: ChangeLogRow, transactionEnds: ReadonlySet<bigint>): Partial<WireEvent> {
  return {
    seq: row.seq.toString(),
    rowId: row.row_id,
    ...(row.tx_id === '' ? {} : { txId: row.tx_id }),
    ...(row.hlc === '' ? {} : { hlc: row.hlc }),
    ...(row.node_id === '' ? {} : { origin: row.node_id }),
    ...(transactionEnds.has(row.seq) ? { txEnd: true } : {}),
  }
}

export function lastOfEachTransaction(log: readonly ChangeLogRow[]): Set<bigint> {
  const last = new Set<bigint>()
  for (const [index, row] of log.entries()) {
    const next = log[index + 1]
    if (row.tx_id === '' || next === undefined || next.tx_id !== row.tx_id) last.add(row.seq)
  }
  return last
}
