import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { InMemoryTransport, MemoryBus } from '../../../transport/memory/index.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationConfig } from '../../types.js'

export function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export interface NodeContext {
  db: Database
  conn: SQLiteConnection
  dbPath: string
  engine: ReplicationEngine
  transport: InMemoryTransport
  tracker: ChangeTracker
}

export const NODE_P = 'pppp0000pppp0000pppp0000pppp0000'
export const NODE_R = 'rrrr0000rrrr0000rrrr0000rrrr0000'
export const NODE_R2 = 'ssss0000ssss0000ssss0000ssss0000'
export const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
export const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

export class SyncTestContext {
  tempDir = ''
  bus: MemoryBus = new MemoryBus()
  readonly openDbs: Database[] = []
  readonly openConns: SQLiteConnection[] = []
  readonly runningEngines: ReplicationEngine[] = []

  setup(): void {
    this.tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sync-'))
    this.bus = new MemoryBus()
  }

  async teardown(): Promise<void> {
    for (const engine of this.runningEngines) {
      try {
        await engine.stop()
      } catch {
        /* best-effort */
      }
    }
    this.runningEngines.length = 0

    for (const db of this.openDbs) {
      try {
        if (!db.closed) await db.close()
      } catch {
        /* best-effort */
      }
    }
    this.openDbs.length = 0

    for (const conn of this.openConns) {
      try {
        await conn.close()
      } catch {
        /* best-effort */
      }
    }
    this.openConns.length = 0

    rmSync(this.tempDir, { recursive: true, force: true })
  }

  async createPrimary(
    nodeId: string,
    tableSqls: string[],
    configOverrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(this.tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    this.openConns.push(conn)

    const tracker = new ChangeTracker()
    for (const sql of tableSqls) {
      await conn.exec(sql)
      const tableName = sql.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i)?.[1]
      if (tableName) {
        await tracker.watch(conn, tableName)
      }
    }

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    this.openDbs.push(db)

    const transport = new InMemoryTransport(this.bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: false,
      snapshotConnectionFactory: () => testDriver.open(dbPath, { readonly: true }),
      changeTracker: tracker,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    this.runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }

  async createReplica(nodeId: string, configOverrides: Partial<ReplicationConfig> = {}): Promise<NodeContext> {
    const dbPath = join(this.tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    this.openConns.push(conn)

    const tracker = new ChangeTracker()

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    this.openDbs.push(db)

    const transport = new InMemoryTransport(this.bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('replica'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: true,
      changeTracker: tracker,
      syncBatchSize: 100,
      syncAckTimeoutMs: 5000,
      catchUpDeadlineMs: 3000,
      maxSyncLagBeforeReady: 10,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    this.runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }

  async createSyncSourceNode(
    nodeId: string,
    tableSqls: string[],
    configOverrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(this.tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    this.openConns.push(conn)

    const tracker = new ChangeTracker()
    for (const sql of tableSqls) {
      await conn.exec(sql)
      const tableName = sql.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i)?.[1]
      if (tableName) {
        await tracker.watch(conn, tableName)
      }
    }

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    this.openDbs.push(db)

    const transport = new InMemoryTransport(this.bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: false,
      snapshotConnectionFactory: () => testDriver.open(dbPath, { readonly: true }),
      changeTracker: tracker,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    this.runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }
}
