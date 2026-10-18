import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { RequestDeniedError } from '../../../core/errors.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, HookConfig, ServerOptions } from '../../../core/types.js'
import { encodeHlc, specChecksum, wireRow } from '../lib/device-sync-oracle.js'
import { RawDeviceClient } from '../lib/raw-device-client.js'
import {
  type RawAnswer,
  readSqlite,
  type ServedRegistry,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

export const DEVICE = 'd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0'
export const OTHER_DEVICE = 'e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0'
export const SESSION_TOKEN = 'session-amara'
export const DATABASE_ID = 'app'
export const NOTES_DDL = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'

export interface Identity {
  userId: string
}

export interface DeviceServer extends ServedRegistry {
  readonly db: Database
  readonly path: string
  readonly clients: RawDeviceClient[]
  connect(): Promise<RawDeviceClient>
}

export interface DeviceServerOptions {
  hooks?: HookConfig
  server?: ServerOptions
  database?: DatabaseOptions
  directory?: string
  driver?: SQLiteDriver
}

export function authenticateSession({ headers }: { headers: Record<string, string | undefined> }): Identity {
  if (headers.authorization !== `Bearer ${SESSION_TOKEN}`) {
    throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Unknown session')
  }
  return { userId: 'u_amara' }
}

export async function openNotesDatabase(
  sirannon: Sirannon,
  path: string,
  options?: DatabaseOptions,
): Promise<Database> {
  const db = await sirannon.open(DATABASE_ID, path, options)
  await db.execute('CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  return db
}

export async function startDeviceServer(options: DeviceServerOptions = {}): Promise<DeviceServer> {
  const path = join(options.directory ?? scratchDirectory('device-sync-server'), `${DATABASE_ID}.db`)
  const sirannon = trackedRegistry({
    ...(options.hooks === undefined ? {} : { hooks: options.hooks }),
    ...(options.driver === undefined ? {} : { driver: options.driver }),
  })
  const db = await openNotesDatabase(sirannon, path, options.database)
  const served = await serveRegistry(sirannon, {
    acceptDeviceSync: true,
    authenticate: authenticateSession,
    ...options.server,
  })
  const clients: RawDeviceClient[] = []
  return {
    ...served,
    db,
    path,
    clients,
    connect: async () => {
      const client = await RawDeviceClient.connect(served.port, DATABASE_ID)
      clients.push(client)
      return client
    },
  }
}

export function destroyClients(server: DeviceServer | undefined): void {
  for (const client of server?.clients.splice(0) ?? []) client.destroy()
}

export interface NativeChange {
  table?: string
  operation?: 'insert' | 'update' | 'delete'
  id: number
  newData?: Record<string, unknown> | null
  oldData?: Record<string, unknown> | null
  hlc?: string
  txId?: string
  nodeId?: string
}

export interface NativeChangeRecord {
  table: string
  operation: string
  rowId: string
  primaryKey: Record<string, unknown>
  hlc: string
  txId: string
  nodeId: string
  newData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
}

export function deviceChange(change: NativeChange): NativeChangeRecord {
  const nodeId = change.nodeId ?? DEVICE
  const operation = change.operation ?? 'insert'
  return {
    table: change.table ?? 'notes',
    operation,
    rowId: String(change.id),
    primaryKey: { id: change.id },
    hlc: change.hlc ?? encodeHlc(Date.now(), 0, nodeId),
    txId: change.txId ?? `tx-${change.id}`,
    nodeId,
    newData:
      change.newData !== undefined
        ? change.newData
        : operation === 'delete'
          ? null
          : { id: change.id, body: `pushed ${change.id}` },
    oldData: change.oldData ?? null,
  }
}

export interface WireBatchOptions {
  fromSeq?: string
  toSeq?: string
  batchId?: string
  hlcRange?: { min: string; max: string }
  checksum?: string
  sourceNodeId?: string
}

export function wireBatch(changes: NativeChangeRecord[], options: WireBatchOptions = {}): Record<string, unknown> {
  const sourceNodeId = options.sourceNodeId ?? changes[0]?.nodeId ?? DEVICE
  const fromSeq = options.fromSeq ?? '1'
  const toSeq = options.toSeq ?? String(BigInt(fromSeq) + BigInt(Math.max(changes.length, 1) - 1))
  const clocks = changes.map(change => change.hlc).sort()
  return {
    sourceNodeId,
    batchId: options.batchId ?? `${sourceNodeId}-${fromSeq}-${toSeq}`,
    fromSeq,
    toSeq,
    hlcRange: options.hlcRange ?? { min: clocks[0] ?? '', max: clocks[clocks.length - 1] ?? '' },
    changes: changes.map(change => ({
      ...change,
      primaryKey: wireRow(change.primaryKey),
      newData: wireRow(change.newData),
      oldData: wireRow(change.oldData),
    })),
    checksum: options.checksum ?? specChecksum(changes),
  }
}

export function pushBody(base: string, body: unknown, token = SESSION_TOKEN): Promise<RawAnswer> {
  return sendRaw(base, 'POST', `/db/${DATABASE_ID}/changes`, JSON.stringify(body), {
    authorization: `Bearer ${token}`,
  })
}

export function push(base: string, batch: Record<string, unknown>, schemaVersion?: number): Promise<RawAnswer> {
  return pushBody(base, schemaVersion === undefined ? { batch } : { schemaVersion, batch })
}

export function postDeviceRoute(base: string, route: string, body: unknown, token = SESSION_TOKEN): Promise<RawAnswer> {
  return sendRaw(base, 'POST', `/db/${DATABASE_ID}/${route}`, JSON.stringify(body), {
    authorization: `Bearer ${token}`,
  })
}

export function storedNotes(path: string): Array<{ id: number; body: string | null }> {
  return readSqlite<{ id: bigint; body: string | null }>(path, 'SELECT id, body FROM notes ORDER BY id').map(row => ({
    id: Number(row.id),
    body: row.body,
  }))
}

export interface LoggedChange {
  seq: bigint
  row_id: string
  operation: string
  node_id: string
  tx_id: string
  hlc: string
}

export function serverChangeLog(path: string): LoggedChange[] {
  return readSqlite<LoggedChange>(
    path,
    'SELECT seq, row_id, operation, node_id, tx_id, hlc FROM _sirannon_changes ORDER BY seq',
  )
}

export function serverMeta(path: string, key: string): string | null {
  const [row] = readSqlite<{ value: string }>(path, 'SELECT value FROM _sirannon_meta WHERE key = ?', [key])
  return row?.value ?? null
}

export function deviceCursors(path: string): Array<{ device_id: string; acked_seq: bigint }> {
  const [exists] = readSqlite(path, "SELECT 1 FROM sqlite_master WHERE name = '_sirannon_device_cursors'")
  if (exists === undefined) return []
  return readSqlite<{ device_id: string; acked_seq: bigint }>(
    path,
    'SELECT device_id, acked_seq FROM _sirannon_device_cursors ORDER BY device_id',
  )
}

export function writeAroundSirannon(path: string, sql: string, params: unknown[] = []): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

export function strictlyRising(seqs: readonly bigint[]): boolean {
  return seqs.every((seq, at) => at === 0 || seq > seqs[at - 1])
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
