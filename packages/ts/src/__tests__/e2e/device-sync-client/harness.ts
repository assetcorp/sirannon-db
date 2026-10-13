import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { SyncController, type SyncControllerOptions } from '../../../client/index.js'
import {
  type Database,
  type Migration,
  Sirannon,
  type SirannonOptions,
  type SQLiteDriver,
} from '../../../core/index.js'
import type { ServerOptions } from '../../../core/server-options.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../../server/index.js'
import { readSqlite, releaseServedRegistries, scratchDirectory, within } from '../lib/served-registry.js'
import { ServerProxy } from '../lib/server-proxy.js'

export const DATABASE_ID = 'app'
export const NOTES_TABLE = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
export const TAGS_TABLE = 'CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)'
export const SETTLE_DEADLINE_MS = 20_000
const POLL_INTERVAL_MS = 10

type Cleanup = () => Promise<void> | void

const controllers: SyncController[] = []
const cleanups: Cleanup[] = []

export function track(cleanup: Cleanup): void {
  cleanups.push(cleanup)
}

export async function releaseDeviceSync(): Promise<void> {
  for (const controller of controllers.splice(0)) await controller.stop().catch(() => {})
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  await releaseServedRegistries()
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function eventually(
  check: () => boolean | Promise<boolean>,
  label: string,
  deadlineMs = SETTLE_DEADLINE_MS,
): Promise<void> {
  await within(
    (async () => {
      while (!(await check())) await sleep(POLL_INTERVAL_MS)
    })(),
    label,
    deadlineMs,
  )
}

export interface SyncServerSetup {
  schema?: readonly string[]
  migrations?: Migration[]
  options?: Partial<ServerOptions>
  hooks?: SirannonOptions['hooks']
  watch?: readonly string[]
}

export interface SyncServer {
  readonly sirannon: Sirannon
  readonly db: Database
  readonly path: string
  readonly url: string
  readonly directPort: number
  readonly proxy: ServerProxy
  stop(): Promise<void>
  start(options?: Partial<ServerOptions>): Promise<void>
  restart(options?: Partial<ServerOptions>): Promise<void>
}

export async function startSyncServer(setup: SyncServerSetup = {}): Promise<SyncServer> {
  const path = join(scratchDirectory('device-sync-server'), 'server.db')
  const sirannon = new Sirannon({ driver: betterSqlite3(), migrations: setup.migrations, hooks: setup.hooks })
  track(() => sirannon.shutdown().catch(() => {}))
  const db = await sirannon.open(DATABASE_ID, path)
  for (const ddl of setup.schema ?? (setup.migrations === undefined ? [NOTES_TABLE] : [])) await db.execute(ddl)
  for (const table of setup.watch ?? ['notes']) await db.watch(table)

  let server: SirannonServer | null = null
  const listen = async (options?: Partial<ServerOptions>): Promise<SirannonServer> => {
    const started = createServer(sirannon, {
      acceptDeviceSync: true,
      authenticate: () => ({}),
      ...setup.options,
      ...options,
      port: 0,
    })
    await started.listen()
    server = started
    return started
  }
  const first = await listen()
  const proxy = new ServerProxy(first.listeningPort)
  await proxy.listen()
  track(async () => {
    await proxy.close()
    await server?.close().catch(() => {})
  })

  const stop = async (): Promise<void> => {
    proxy.killAllConnections()
    await server?.close()
    server = null
  }
  const start = async (options?: Partial<ServerOptions>): Promise<void> => {
    const started = await listen(options)
    proxy.pointAt(started.listeningPort)
  }

  return {
    sirannon,
    db,
    path,
    url: `http://127.0.0.1:${proxy.port}`,
    get directPort() {
      return server?.listeningPort ?? 0
    },
    proxy,
    stop,
    start,
    async restart(options?: Partial<ServerOptions>) {
      await stop()
      await start(options)
    },
  }
}

export interface DeviceSetup {
  schema?: readonly string[]
  migrations?: Migration[]
  watch?: readonly string[]
  driver?: SQLiteDriver
  path?: string
}

export interface SyncDevice {
  readonly path: string
  readonly sirannon: Sirannon
  readonly db: Database
  sync(options?: Partial<SyncControllerOptions>): SyncController
  reopen(): Promise<Database>
}

export async function openDevice(serverUrl: string, setup: DeviceSetup = {}): Promise<SyncDevice> {
  const path = setup.path ?? join(scratchDirectory('device-sync-device'), 'device.db')
  const driver = setup.driver ?? betterSqlite3()
  const own: SyncController[] = []
  let sirannon = new Sirannon({ driver })
  let db = await sirannon.open(DATABASE_ID, path)
  track(() => sirannon.shutdown().catch(() => {}))
  if (setup.migrations !== undefined) await db.migrate(setup.migrations)
  for (const ddl of setup.schema ?? (setup.migrations === undefined ? [NOTES_TABLE] : [])) await db.execute(ddl)
  for (const table of setup.watch ?? ['notes']) await db.watch(table)

  return {
    path,
    get sirannon() {
      return sirannon
    },
    get db() {
      return db
    },
    sync(options?: Partial<SyncControllerOptions>) {
      const controller = new SyncController(db, {
        url: serverUrl,
        databaseId: DATABASE_ID,
        tables: ['notes'],
        pushIntervalMs: 25,
        ackIntervalMs: 25,
        maxPushRetryDelayMs: 250,
        ...options,
      })
      controllers.push(controller)
      own.push(controller)
      return controller
    },
    async reopen() {
      for (const controller of own.splice(0)) await controller.stop().catch(() => {})
      await sirannon.shutdown()
      sirannon = new Sirannon({ driver })
      track(() => sirannon.shutdown().catch(() => {}))
      db = await sirannon.open(DATABASE_ID, path)
      return db
    },
  }
}

export function writeSqlite(path: string, statements: ReadonlyArray<[sql: string, params?: unknown[]]>): void {
  const raw = new BetterSqlite3(path)
  try {
    for (const [sql, params] of statements) raw.prepare(sql).run(...(params ?? []))
  } finally {
    raw.close()
  }
}

export type Row = Record<string, unknown>

export function rowsOf(path: string, table: string): Row[] {
  return readSqlite<Row>(path, `SELECT * FROM "${table}" ORDER BY rowid`)
}

export function countOf(path: string, table: string): number {
  if (!tableExists(path, table)) return 0
  const [row] = readSqlite<{ n: bigint }>(path, `SELECT count(*) AS n FROM "${table}"`)
  return Number(row?.n ?? 0n)
}

export function tableExists(path: string, table: string): boolean {
  return readSqlite(path, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [table]).length === 1
}

export function stagedCount(path: string): number {
  return countOf(path, '_sirannon_staged_changes')
}

export function metaValue(path: string, key: string): string | null {
  if (!tableExists(path, '_sirannon_meta')) return null
  const [row] = readSqlite<{ value: string }>(path, 'SELECT value FROM _sirannon_meta WHERE key = ?', [key])
  return row?.value ?? null
}

export function sameRows(left: string, right: string, table: string): boolean {
  if (!tableExists(left, table) || !tableExists(right, table)) return false
  const a = rowsOf(left, table)
  const b = rowsOf(right, table)
  return a.length === b.length && a.every((row, index) => rowsEqual(row, b[index]))
}

function rowsEqual(left: Row, right: Row | undefined): boolean {
  if (right === undefined) return false
  const keys = Object.keys(left)
  if (keys.length !== Object.keys(right).length) return false
  return keys.every(key => valuesEqual(left[key], right[key]))
}

function valuesEqual(left: unknown, right: unknown): boolean {
  if (Buffer.isBuffer(left) && Buffer.isBuffer(right)) return left.equals(right)
  return left === right
}

export interface CountSampler {
  readonly samples: number[]
  stop(): number[]
}

export function sampleCounts(path: string, sql: string, params: unknown[] = []): CountSampler {
  const samples: number[] = []
  let running = true
  const tick = (): void => {
    if (!running) return
    const [row] = readSqlite<{ n: bigint }>(path, sql, params)
    samples.push(Number(row?.n ?? -1n))
    setTimeout(tick, 2)
  }
  tick()
  return {
    samples,
    stop() {
      running = false
      return samples
    },
  }
}
