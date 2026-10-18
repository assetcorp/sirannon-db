import { rmSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import type { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { ReplicationConfig, ReplicationErrorEvent } from '../../../replication/index.js'
import type { GrpcReplicationTransport } from '../../../transport/grpc/index.js'
import type { MtlsCerts } from './certs.js'
import { createPrimary, createReplica, type ManagedNode, type NodeStorage, stopNode } from './factory.js'
import { type FaultProxy, startFaultProxy } from './fault-proxy.js'

const POLL_INTERVAL_MS = 10
const DEFAULT_DEADLINE_MS = 15_000
const SAFE_LOW = BigInt(Number.MIN_SAFE_INTEGER)
const SAFE_HIGH = BigInt(Number.MAX_SAFE_INTEGER)

export type Row = Record<string, unknown>

export interface PrimaryOptions {
  schema?: readonly string[]
  watch?: readonly string[]
  config?: Partial<ReplicationConfig>
  storage?: NodeStorage
  listenPort?: number
  driver?: SQLiteDriver
  registryOptions?: Partial<SirannonOptions>
  initialize?: (conn: SQLiteConnection, tracker: ChangeTracker) => Promise<void>
}

export interface ReplicaOptions {
  port: number
  config?: Partial<ReplicationConfig>
  storage?: NodeStorage
  driver?: SQLiteDriver
  registryOptions?: Partial<SirannonOptions>
}

export async function eventually(
  condition: () => boolean | Promise<boolean>,
  label: string,
  deadlineMs = DEFAULT_DEADLINE_MS,
): Promise<void> {
  const deadline = Date.now() + deadlineMs
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error(`Timed out after ${deadlineMs} ms waiting for ${label}`)
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS))
  }
}

function plainValue(value: unknown): unknown {
  if (typeof value === 'bigint' && value >= SAFE_LOW && value <= SAFE_HIGH) return Number(value)
  return value
}

export function readRows(path: string, sql: string, params: readonly unknown[] = []): Row[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    raw.defaultSafeIntegers(true)
    const rows = raw.prepare(sql).all(...params) as Row[]
    return rows.map(row => Object.fromEntries(Object.entries(row).map(([key, value]) => [key, plainValue(value)])))
  } finally {
    raw.close()
  }
}

export function tableRows(path: string, table: string, orderBy = 'id'): Row[] {
  return readRows(path, `SELECT * FROM "${table}" ORDER BY ${orderBy}`)
}

export function tableNames(path: string): string[] {
  return readRows(
    path,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sirannon\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
  ).map(row => String(row.name))
}

export function columnNames(path: string, table: string): string[] {
  return readRows(path, `SELECT name FROM pragma_table_info('${table}') ORDER BY cid`).map(row => String(row.name))
}

export function indexNames(path: string, table: string): string[] {
  return readRows(
    path,
    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name",
    [table],
  ).map(row => String(row.name))
}

export function errorsOf(node: ManagedNode, operation?: string): ReplicationErrorEvent[] {
  return node.recentErrors.filter(event => operation === undefined || event.operation === operation)
}

export function storageOf(node: ManagedNode): NodeStorage {
  return { tempDir: node.tempDir, dbPath: node.dbPath }
}

export function recordErrors(node: ManagedNode): ReplicationErrorEvent[] {
  const seen: ReplicationErrorEvent[] = []
  node.engine.on('replication-error', (event: ReplicationErrorEvent) => seen.push(event))
  return seen
}

export async function caughtUp(replica: ManagedNode, primary: ManagedNode, deadlineMs = DEFAULT_DEADLINE_MS) {
  const target = primary.engine.getCurrentSeq()
  await eventually(
    () => replica.engine.getAppliedSeq(primary.nodeId) >= target,
    `${replica.nodeId} to apply ${primary.nodeId} up to ${target}`,
    deadlineMs,
  )
}

export class ReplicationScene {
  private readonly running = new Set<ManagedNode>()
  private readonly registries = new Set<Sirannon>()
  private readonly proxies = new Set<FaultProxy>()
  private readonly transports = new Set<GrpcReplicationTransport>()
  private readonly directories = new Set<string>()

  constructor(readonly certs: MtlsCerts) {}

  async primary(nodeId: string, options: PrimaryOptions = {}): Promise<ManagedNode> {
    const driver = options.driver ?? betterSqlite3()
    const sirannon = this.registry(driver, options.registryOptions)
    const node = await createPrimary({
      nodeId,
      certs: this.certs,
      storage: options.storage,
      listenPort: options.listenPort,
      configOverrides: options.config,
      registry: { sirannon, driver, databaseId: nodeId },
      initialize: async (conn, tracker) => {
        for (const statement of options.schema ?? []) await conn.exec(statement)
        for (const table of options.watch ?? []) await tracker.watch(conn, table)
        await options.initialize?.(conn, tracker)
      },
    })
    this.adopt(node)
    return node
  }

  async replica(nodeId: string, options: ReplicaOptions): Promise<ManagedNode> {
    const driver = options.driver ?? betterSqlite3()
    const sirannon = this.registry(driver, options.registryOptions)
    const node = await createReplica({
      nodeId,
      certs: this.certs,
      primaryHost: 'localhost',
      primaryPort: options.port,
      storage: options.storage,
      configOverrides: options.config,
      registry: { sirannon, driver, databaseId: nodeId },
    })
    this.adopt(node)
    return node
  }

  async proxy(targetPort: number): Promise<FaultProxy> {
    const proxy = await startFaultProxy(targetPort)
    this.proxies.add(proxy)
    return proxy
  }

  track(transport: GrpcReplicationTransport): GrpcReplicationTransport {
    this.transports.add(transport)
    return transport
  }

  async stop(node: ManagedNode): Promise<NodeStorage> {
    this.running.delete(node)
    await stopNode(node)
    return storageOf(node)
  }

  async teardown(): Promise<void> {
    for (const transport of this.transports) await transport.disconnect().catch(() => {})
    this.transports.clear()
    for (const node of [...this.running].reverse()) await stopNode(node)
    this.running.clear()
    for (const sirannon of this.registries) await sirannon.shutdown().catch(() => {})
    this.registries.clear()
    for (const proxy of this.proxies) await proxy.close().catch(() => {})
    this.proxies.clear()
    for (const directory of this.directories) rmSync(directory, { recursive: true, force: true })
    this.directories.clear()
  }

  private registry(driver: SQLiteDriver, options: Partial<SirannonOptions> = {}): Sirannon {
    const sirannon = new Sirannon({ ...options, driver })
    this.registries.add(sirannon)
    return sirannon
  }

  private adopt(node: ManagedNode): void {
    this.running.add(node)
    this.directories.add(node.tempDir)
  }
}
