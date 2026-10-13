import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { RequestContext, ServerOptions, SirannonOptions } from '../../../core/types.js'
import {
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'
import { ServerProxy } from '../lib/server-proxy.js'
import { DroppingListener, RecordingHttpProxy } from './http-recorder.js'
import { WebSocketMiddlebox } from './middlebox.js'

export const ORDERS_SCHEMA =
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)'
export const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
export const DATABASE_ID = 'shop'

export interface StoredOrder {
  id: number
  customer: string
  total: number
}

export interface ServedShop {
  readonly sirannon: Sirannon
  readonly database: Database
  readonly path: string
  readonly served: ServedRegistry
}

export interface ShopOptions {
  server?: ServerOptions
  registry?: Partial<SirannonOptions>
  customers?: string[]
  label?: string
}

const closers: Array<() => Promise<void> | void> = []

export async function servedShop(options: ShopOptions = {}): Promise<ServedShop> {
  const path = join(scratchDirectory(options.label ?? 'client'), 'shop.db')
  const sirannon = trackedRegistry(options.registry)
  const database = await sirannon.open(DATABASE_ID, path)
  await database.execute(ORDERS_SCHEMA)
  const customers = options.customers ?? ['ama', 'kofi']
  if (customers.length > 0) {
    await database.executeBatch(
      INSERT_ORDER,
      customers.map((customer, index) => [customer, (index + 1) * 10]),
    )
  }
  const served = await serveRegistry(sirannon, { acceptSql: true, ...options.server })
  return { sirannon, database, path, served }
}

export async function restartServer(shop: ServedShop, options: ServerOptions = {}): Promise<ServedRegistry> {
  await shop.served.server.close()
  return serveRegistry(shop.sirannon, { acceptSql: true, ...options })
}

export function storedOrders(path: string): StoredOrder[] {
  return readSqlite<{ id: bigint; customer: string; total: bigint }>(
    path,
    'SELECT id, customer, total FROM orders ORDER BY id',
  ).map(row => ({ id: Number(row.id), customer: row.customer, total: Number(row.total) }))
}

export function storedCustomers(path: string): string[] {
  return storedOrders(path).map(order => order.customer)
}

export function changeLogSeqs(path: string, table: string): bigint[] {
  return readSqlite<{ seq: bigint }>(path, 'SELECT seq FROM _sirannon_changes WHERE table_name = ? ORDER BY seq', [
    table,
  ]).map(row => row.seq)
}

export function pruneChangeLog(path: string, throughSeq: bigint): number {
  const raw = new BetterSqlite3(path, { fileMustExist: true, timeout: 5_000 })
  try {
    return raw.prepare('DELETE FROM _sirannon_changes WHERE seq <= ?').run(throughSeq).changes
  } finally {
    raw.close()
  }
}

export interface RequestTally {
  readonly upgrades: RequestContext[]
  readonly requests: RequestContext[]
  readonly authenticate: (ctx: RequestContext) => undefined
}

export function requestTally(): RequestTally {
  const upgrades: RequestContext[] = []
  const requests: RequestContext[] = []
  return {
    upgrades,
    requests,
    authenticate: ctx => {
      if (ctx.headers['sec-websocket-key'] !== undefined) upgrades.push(ctx)
      else requests.push(ctx)
      return undefined
    },
  }
}

export async function serverProxy(targetPort: number): Promise<ServerProxy> {
  const proxy = new ServerProxy(targetPort)
  await proxy.listen()
  closers.push(() => proxy.close())
  return proxy
}

export async function middlebox(targetPort: number): Promise<WebSocketMiddlebox> {
  const box = new WebSocketMiddlebox(targetPort)
  await box.listen()
  closers.push(() => box.close())
  return box
}

export async function recordingProxy(targetPort: number | null): Promise<RecordingHttpProxy> {
  const proxy = new RecordingHttpProxy(targetPort)
  await proxy.listen()
  closers.push(() => proxy.close())
  return proxy
}

export async function droppingListener(): Promise<DroppingListener> {
  const listener = new DroppingListener()
  await listener.listen()
  closers.push(() => listener.close())
  return listener
}

export function closeLater(close: () => Promise<void> | void): void {
  closers.push(close)
}

export async function releaseClientServers(): Promise<void> {
  for (const close of closers.splice(0).reverse()) await close()
  await releaseServedRegistries()
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export interface Gate {
  readonly opened: Promise<void>
  open(): void
}

export function gate(): Gate {
  let open = () => {}
  const opened = new Promise<void>(resolve => {
    open = resolve
  })
  return { opened, open }
}
