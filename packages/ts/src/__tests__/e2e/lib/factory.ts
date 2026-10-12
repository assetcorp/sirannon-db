import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { ReplicationEngine } from '../../../replication/engine.js'
import { PrimaryReplicaTopology } from '../../../replication/topology/primary-replica.js'
import type { ReplicationConfig, ReplicationErrorEvent } from '../../../replication/types.js'
import { GrpcReplicationTransport } from '../../../transport/grpc/index.js'
import type { MtlsCerts } from './certs.js'

export interface ManagedNode {
  nodeId: string
  db: Database
  conn: SQLiteConnection
  engine: ReplicationEngine
  transport: GrpcReplicationTransport
  tracker: ChangeTracker
  tempDir: string
  dbPath: string
  port: number
  recentErrors: readonly ReplicationErrorEvent[]
}

export interface NodeStorage {
  tempDir: string
  dbPath: string
}

export interface NodeRegistry {
  sirannon: Sirannon
  driver: SQLiteDriver
  databaseId: string
}

export interface CreatePrimaryArgs {
  nodeId: string
  certs: MtlsCerts
  initialize?: (conn: SQLiteConnection, tracker: ChangeTracker) => Promise<void>
  configOverrides?: Partial<ReplicationConfig>
  listenPort?: number
  storage?: NodeStorage
  registry?: NodeRegistry
}

export interface CreateReplicaArgs {
  nodeId: string
  certs: MtlsCerts
  primaryHost: string
  primaryPort: number
  configOverrides?: Partial<ReplicationConfig>
  storage?: NodeStorage
  registry?: NodeRegistry
}

function driverOf(registry: NodeRegistry | undefined): SQLiteDriver {
  return registry?.driver ?? testDriver
}

async function openDatabase(nodeId: string, dbPath: string, registry: NodeRegistry | undefined): Promise<Database> {
  if (registry) return registry.sirannon.open(registry.databaseId, dbPath)
  return Database.create(`db-${nodeId}`, dbPath, testDriver)
}

const ERROR_BUFFER_LIMIT = 20

function createTempDir(): string {
  return mkdtempSync(join(tmpdir(), 'sirannon-e2e-'))
}

function createNodeStorage(nodeId: string, storage?: NodeStorage): NodeStorage {
  if (storage) {
    return storage
  }

  const tempDir = createTempDir()
  return {
    tempDir,
    dbPath: join(tempDir, `${nodeId}.db`),
  }
}

function attachErrorBuffer(engine: ReplicationEngine): { recent: readonly ReplicationErrorEvent[] } {
  const buffer: ReplicationErrorEvent[] = []
  engine.on('replication-error', (event: ReplicationErrorEvent) => {
    buffer.push(event)
    if (buffer.length > ERROR_BUFFER_LIMIT) {
      buffer.shift()
    }
  })
  return { recent: buffer }
}

export async function createPrimary(args: CreatePrimaryArgs): Promise<ManagedNode> {
  const storage = createNodeStorage(args.nodeId, args.storage)
  const { tempDir, dbPath } = storage

  const driver = driverOf(args.registry)
  const conn = await driver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')

  const tracker = new ChangeTracker()
  if (args.initialize) {
    await args.initialize(conn, tracker)
  }

  const db = await openDatabase(args.nodeId, dbPath, args.registry)

  const cert = args.certs.certForNode(args.nodeId)
  const transport = new GrpcReplicationTransport({
    port: args.listenPort ?? 0,
    host: 'localhost',
    tlsCert: cert.certPath,
    tlsKey: cert.keyPath,
    tlsCaCert: args.certs.caCertPath,
  })

  const config: ReplicationConfig = {
    nodeId: args.nodeId,
    topology: new PrimaryReplicaTopology('primary'),
    transport,
    batchIntervalMs: 30,
    batchSize: 200,
    maxBatchChanges: 5_000,
    initialSync: false,
    changeTracker: tracker,
    snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
    syncBatchSize: 500,
    syncAckTimeoutMs: 10_000,
    ...args.configOverrides,
  }

  const engine = new ReplicationEngine(db, conn, config)
  const errorBuffer = attachErrorBuffer(engine)

  await engine.start()
  const port = transport.getPort()

  return {
    nodeId: args.nodeId,
    db,
    conn,
    engine,
    transport,
    tracker,
    tempDir,
    dbPath,
    port,
    get recentErrors() {
      return errorBuffer.recent
    },
  }
}

export async function createReplica(args: CreateReplicaArgs): Promise<ManagedNode> {
  const storage = createNodeStorage(args.nodeId, args.storage)
  const { tempDir, dbPath } = storage

  const conn = await driverOf(args.registry).open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')

  const tracker = new ChangeTracker()
  const db = await openDatabase(args.nodeId, dbPath, args.registry)

  const cert = args.certs.certForNode(args.nodeId)
  const transport = new GrpcReplicationTransport({
    host: 'localhost',
    tlsCert: cert.certPath,
    tlsKey: cert.keyPath,
    tlsCaCert: args.certs.caCertPath,
  })

  const config: ReplicationConfig = {
    nodeId: args.nodeId,
    topology: new PrimaryReplicaTopology('replica'),
    transport,
    transportConfig: {
      localRole: 'replica',
      endpoints: [`${args.primaryHost}:${args.primaryPort}`],
    },
    batchIntervalMs: 30,
    batchSize: 200,
    maxBatchChanges: 5_000,
    initialSync: true,
    changeTracker: tracker,
    syncBatchSize: 500,
    syncAckTimeoutMs: 10_000,
    catchUpDeadlineMs: 10_000,
    maxSyncLagBeforeReady: 10,
    writeForwarding: true,
    ...args.configOverrides,
  }

  const engine = new ReplicationEngine(db, conn, config)
  const errorBuffer = attachErrorBuffer(engine)

  await engine.start()

  return {
    nodeId: args.nodeId,
    db,
    conn,
    engine,
    transport,
    tracker,
    tempDir,
    dbPath,
    port: 0,
    get recentErrors() {
      return errorBuffer.recent
    },
  }
}

export async function stopNode(node: ManagedNode): Promise<void> {
  try {
    await node.engine.stop()
  } catch {}
  try {
    if (!node.db.closed) {
      await node.db.close()
    }
  } catch {}
  try {
    await node.conn.close()
  } catch {}
}
