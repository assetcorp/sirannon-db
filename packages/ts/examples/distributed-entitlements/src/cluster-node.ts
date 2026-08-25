import { readFileSync } from 'node:fs'
import {
  ChangeTracker,
  RequestDeniedError,
  readBearerToken,
  readSubprotocolCredential,
  Sirannon,
} from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import {
  PrimaryReplicaTopology,
  ReplicationEngine,
  toClusterStatusInfo,
  toReplicationStatusInfo,
} from '@delali/sirannon-db/replication'
import { createEtcdCoordinator } from '@delali/sirannon-db/replication/coordinator/etcd'
import { createServer, type SirannonServer } from '@delali/sirannon-db/server'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'
import {
  NODE_IDS,
  numberEnv,
  requireClusterToken,
  requireCsv,
  requireEnv,
  requireNodeId,
  requirePort,
  requireRole,
} from './cluster-node-env'
import { REPLICATED_TABLES, SCHEMA, SEED_SQL } from './cluster-node-schema'
import { WEBSOCKET_AUTH_PROTOCOL_PREFIX } from './lib/cluster-config'
import { type ControlPlaneOperator, operations } from './operations'

const DATABASE_ID = 'entitlements'
const CLUSTER_ID = 'sirannon-entitlements-control-plane'
const GROUP_ID = 'entitlements'
const DEFAULT_SESSION_TTL_MS = 10_000
const DEFAULT_CONTROLLER_LEASE_TTL_MS = 5_000
const DEFAULT_CONTROLLER_TICK_MS = 1_000
const DEFAULT_HTTP_ENDPOINTS: Record<string, string> = {
  'node-a': 'http://127.0.0.1:7301/db/entitlements',
  'node-b': 'http://127.0.0.1:7302/db/entitlements',
  'node-c': 'http://127.0.0.1:7303/db/entitlements',
}

const nodeId = requireNodeId()
const initialRole = requireRole()
const dbPath = requireEnv('DB_PATH')
const httpHost = process.env.HTTP_HOST ?? '127.0.0.1'
const httpPort = requirePort('HTTP_PORT')
const grpcHost = process.env.GRPC_HOST ?? '127.0.0.1'
const grpcPort = requirePort('GRPC_PORT')
const endpoints = requireCsv('GRPC_ENDPOINTS')
const etcdHosts = requireCsv('ETCD_ENDPOINTS')
const token = requireClusterToken()
const seedSchema = process.env.SEED_SCHEMA === 'true'
const httpEndpoints = { ...DEFAULT_HTTP_ENDPOINTS, [nodeId]: requireEnv('HTTP_PUBLIC_ENDPOINT') }
const driver = betterSqlite3({ busyTimeout: 10_000 })

const conn = await driver.open(dbPath)
await conn.exec('PRAGMA journal_mode = WAL')

const tracker = new ChangeTracker()
if (seedSchema) {
  await conn.exec(SCHEMA)
  await conn.exec(SEED_SQL)
}

const replicatedTablePlaceholders = REPLICATED_TABLES.map(() => '?').join(', ')
const existingReplicatedTableRows = (await (
  await conn.prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (${replicatedTablePlaceholders})`)
).all(...REPLICATED_TABLES)) as Array<{ name: unknown }>
const existingReplicatedTables = new Set<string>()

for (const row of existingReplicatedTableRows) {
  if (typeof row.name === 'string') {
    existingReplicatedTables.add(row.name)
  }
}

for (const table of REPLICATED_TABLES) {
  if (existingReplicatedTables.has(table)) {
    await tracker.watch(conn, table)
  }
}

const sirannon = new Sirannon({ driver })
const db = await sirannon.open(DATABASE_ID, dbPath)
const transport = new GrpcReplicationTransport({
  host: grpcHost,
  port: grpcPort,
  tlsCert: requireEnv('TLS_CERT'),
  tlsKey: requireEnv('TLS_KEY'),
  tlsCaCert: requireEnv('TLS_CA_CERT'),
})
const coordinator = createEtcdCoordinator({
  hosts: etcdHosts,
  keyPrefix: process.env.ETCD_KEY_PREFIX ?? '/sirannon/examples/entitlements',
  allowInsecure: true,
  dialTimeoutMs: 5_000,
  defaultCallTimeoutMs: 3_000,
})

const engine = new ReplicationEngine(db, conn, {
  nodeId,
  topology: new PrimaryReplicaTopology(initialRole),
  transport,
  transportConfig: {
    localRole: initialRole,
    endpoints,
    groupId: GROUP_ID,
    protocolVersion: '1',
  },
  batchIntervalMs: 25,
  batchSize: 100,
  maxBatchChanges: 1_000,
  maxPendingBatches: 4,
  ackTimeoutMs: 1_500,
  initialSync: true,
  changeTracker: tracker,
  snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
  syncBatchSize: 100,
  syncAckTimeoutMs: 5_000,
  catchUpDeadlineMs: 8_000,
  maxSyncLagBeforeReady: 0,
  writeForwarding: true,
  coordinator: {
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    endpoint: httpEndpoints[nodeId],
    coordinator,
    votingDataBearingNodeIds: seedSchema ? [...NODE_IDS] : undefined,
    sessionTtlMs: numberEnv('SESSION_TTL_MS', DEFAULT_SESSION_TTL_MS),
    controller: {
      enabled: true,
      leaseTtlMs: numberEnv('CONTROLLER_LEASE_TTL_MS', DEFAULT_CONTROLLER_LEASE_TTL_MS),
      tickIntervalMs: numberEnv('CONTROLLER_TICK_MS', DEFAULT_CONTROLLER_TICK_MS),
    },
    compatibility: {
      packageVersion: sirannonPackageVersion(),
      specVersion: 'coordinator-mode-example',
      protocolVersion: '1',
    },
  },
})

let server: SirannonServer | null = null

engine.on('replication-error', event => {
  const severity = event.recoverable ? 'recoverable' : 'fatal'
  const peer = event.peerId === undefined ? '' : ` peer=${event.peerId}`
  console.error(`[${nodeId}] replication ${severity} in ${event.operation}${peer}: ${event.error.message}`)
  if (event.error.stack !== undefined) {
    console.error(event.error.stack)
  }
})

await engine.start()

server = createServer<ControlPlaneOperator>(sirannon, {
  host: httpHost,
  port: httpPort,
  cors: {
    origin: ['http://127.0.0.1:3001', 'http://localhost:3001'],
    methods: ['GET', 'POST', 'OPTIONS'],
    headers: ['Content-Type', 'Authorization'],
  },
  operations,
  authenticate: ctx => {
    if (readBearerToken(ctx) === token) {
      return { actor: 'control-plane-operator' }
    }

    if (readSubprotocolCredential(ctx, WEBSOCKET_AUTH_PROTOCOL_PREFIX) === token) {
      return { actor: 'control-plane-browser' }
    }

    throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Missing valid Sirannon entitlements demo token')
  },
  authorizeClusterStatus: ctx => readBearerToken(ctx) === token,
  resolveExecutionTarget: id => (id === DATABASE_ID ? engine : null),
  getReplicationStatus: () => toReplicationStatusInfo(engine.status()),
  getClusterStatus: id =>
    id === DATABASE_ID
      ? toClusterStatusInfo(engine.status(), { databaseId: DATABASE_ID, endpoints: httpEndpoints })
      : null,
})

await server.listen()
console.log(`Sirannon entitlements ${nodeId} listening on ${httpHost}:${httpPort}`)

process.on('SIGTERM', () => {
  shutdown().finally(() => process.exit(0))
})

process.on('SIGINT', () => {
  shutdown().finally(() => process.exit(0))
})

async function shutdown(): Promise<void> {
  await server?.close().catch(() => undefined)
  server = null
  await engine.stop().catch(() => undefined)
  await coordinator.close().catch(() => undefined)
  await db.close().catch(() => undefined)
  await sirannon.shutdown().catch(() => undefined)
  await conn.close().catch(() => undefined)
}

function sirannonPackageVersion(): string {
  const manifestUrl = new URL('../../../package.json', import.meta.url)
  const manifest = JSON.parse(readFileSync(manifestUrl, 'utf8')) as { version?: unknown }
  if (typeof manifest.version !== 'string') {
    throw new Error(`No version found in ${manifestUrl.pathname}`)
  }
  return manifest.version
}
