import { ChangeTracker, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
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
import { type ClusterStatusContext, toClusterStatusInfo, toReplicationStatusInfo } from './cluster-node-status'

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
const statusContext: ClusterStatusContext = { databaseId: DATABASE_ID, nodeId, httpEndpoints }
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
  dialTimeoutMs: 1_000,
  defaultCallTimeoutMs: 1_000,
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
      packageVersion: '0.1.4',
      specVersion: 'coordinator-mode-example',
      protocolVersion: '1',
    },
  },
})

let server: SirannonServer | null = null
const recentErrors: unknown[] = []

engine.on('replication-error', event => {
  recentErrors.push(toJsonSafe(event))
  if (recentErrors.length > 30) {
    recentErrors.shift()
  }
})

await engine.start()

server = createServer(sirannon, {
  host: httpHost,
  port: httpPort,
  cors: {
    origin: ['http://127.0.0.1:3001', 'http://localhost:3001'],
    methods: ['GET', 'POST', 'OPTIONS'],
    headers: ['Content-Type', 'Authorization'],
  },
  onRequest: ({ headers }) => {
    if (isAuthorized(headers, token)) {
      return undefined
    }
    return {
      status: 401,
      code: 'UNAUTHORIZED',
      message: 'Missing valid Sirannon entitlements demo token',
    }
  },
  resolveExecutionTarget: id => (id === DATABASE_ID ? engine : null),
  getReplicationStatus: () => toReplicationStatusInfo(engine.status()),
  getClusterStatus: id => toClusterStatusInfo(id, engine.status(), statusContext),
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

function isAuthorized(headers: Record<string, string>, expectedToken: string): boolean {
  const authorization = headers.authorization
  if (authorization === `Bearer ${expectedToken}`) {
    return true
  }

  const protocols = (headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
  return protocols.includes(toWebSocketAuthProtocol(expectedToken))
}

function toWebSocketAuthProtocol(value: string): string {
  return `sirannon.entitlements.auth.${Buffer.from(value, 'utf8').toString('base64url')}`
}

function toJsonSafe(value: unknown): unknown {
  return JSON.parse(
    JSON.stringify(value, (_key, nestedValue) =>
      typeof nestedValue === 'bigint' ? nestedValue.toString() : nestedValue,
    ),
  )
}
