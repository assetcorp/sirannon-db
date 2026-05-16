import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
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

export interface CreatePrimaryArgs {
  nodeId: string
  certs: MtlsCerts
  initialize?: (conn: SQLiteConnection, tracker: ChangeTracker) => Promise<void>
  configOverrides?: Partial<ReplicationConfig>
}

export interface CreateReplicaArgs {
  nodeId: string
  certs: MtlsCerts
  primaryHost: string
  primaryPort: number
  configOverrides?: Partial<ReplicationConfig>
}

const ERROR_BUFFER_LIMIT = 20

function createTempDir(): string {
  return mkdtempSync(join(tmpdir(), 'sirannon-e2e-'))
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
  const tempDir = createTempDir()
  const dbPath = join(tempDir, `${args.nodeId}.db`)

  const conn = await testDriver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')

  const tracker = new ChangeTracker({ replication: true })
  if (args.initialize) {
    await args.initialize(conn, tracker)
  }

  const db = await Database.create(`db-${args.nodeId}`, dbPath, testDriver)

  const cert = args.certs.certForNode(args.nodeId)
  const transport = new GrpcReplicationTransport({
    port: 0,
    host: '127.0.0.1',
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
    snapshotConnectionFactory: () => testDriver.open(dbPath, { readonly: true }),
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
  const tempDir = createTempDir()
  const dbPath = join(tempDir, `${args.nodeId}.db`)

  const conn = await testDriver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')

  const tracker = new ChangeTracker({ replication: true })
  const db = await Database.create(`db-${args.nodeId}`, dbPath, testDriver)

  const cert = args.certs.certForNode(args.nodeId)
  const transport = new GrpcReplicationTransport({
    host: '127.0.0.1',
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
  } catch {
    /* best-effort */
  }
  try {
    if (!node.db.closed) {
      await node.db.close()
    }
  } catch {
    /* best-effort */
  }
  try {
    await node.conn.close()
  } catch {
    /* best-effort */
  }
}
