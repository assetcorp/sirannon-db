import { ChangeTracker, type ClusterStatusInfo, type ReplicationStatusInfo, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { PrimaryReplicaTopology, ReplicationEngine, type ReplicationStatus } from '@delali/sirannon-db/replication'
import { createEtcdCoordinator } from '@delali/sirannon-db/replication/coordinator/etcd'
import { createServer, type SirannonServer } from '@delali/sirannon-db/server'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const DATABASE_ID = 'entitlements'
const CLUSTER_ID = 'sirannon-entitlements-control-plane'
const GROUP_ID = 'entitlements'
const NODE_IDS = ['node-a', 'node-b', 'node-c'] as const
const DEFAULT_SESSION_TTL_MS = 10_000
const DEFAULT_CONTROLLER_LEASE_TTL_MS = 5_000
const DEFAULT_CONTROLLER_TICK_MS = 1_000
const DEFAULT_HTTP_ENDPOINTS: Record<string, string> = {
  'node-a': 'http://127.0.0.1:7301/db/entitlements',
  'node-b': 'http://127.0.0.1:7302/db/entitlements',
  'node-c': 'http://127.0.0.1:7303/db/entitlements',
}
const REPLICATED_TABLES = ['customers', 'entitlements', 'usage_events', 'billing_events', 'audit_log'] as const

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    plan TEXT NOT NULL CHECK (plan IN ('free', 'growth', 'scale', 'enterprise')),
    status TEXT NOT NULL CHECK (status IN ('active', 'past_due', 'suspended')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS entitlements (
    customer_id INTEGER PRIMARY KEY,
    seats INTEGER NOT NULL CHECK (seats >= 0),
    api_quota INTEGER NOT NULL CHECK (api_quota >= 0),
    support_tier TEXT NOT NULL CHECK (support_tier IN ('community', 'standard', 'priority', 'named')),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS usage_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    source TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS billing_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    customer_external_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'duplicate', 'stale'))
  );

  CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`

const SEED_SQL = `
  INSERT OR IGNORE INTO customers (external_id, name, plan, status) VALUES
    ('cus_nova_forge', 'Nova Forge', 'scale', 'active'),
    ('cus_helios_labs', 'Helios Labs', 'growth', 'active'),
    ('cus_riverline_ai', 'Riverline AI', 'enterprise', 'active');

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 48, 250000, 'priority', 1, 8 FROM customers WHERE external_id = 'cus_nova_forge';

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 18, 75000, 'standard', 1, 4 FROM customers WHERE external_id = 'cus_helios_labs';

  INSERT OR IGNORE INTO entitlements (customer_id, seats, api_quota, support_tier, active, version)
  SELECT id, 120, 900000, 'named', 1, 12 FROM customers WHERE external_id = 'cus_riverline_ai';

  INSERT INTO audit_log (actor, action, target, detail)
  SELECT 'seed', 'seeded', 'control-plane', 'Loaded production-style entitlement records'
  WHERE NOT EXISTS (SELECT 1 FROM audit_log WHERE actor = 'seed' AND action = 'seeded');
`

type NodeRole = 'primary' | 'replica'

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

const tracker = new ChangeTracker({ replication: true })
if (seedSchema) {
  await conn.exec(SCHEMA)
  await conn.exec(SEED_SQL)
  for (const table of REPLICATED_TABLES) {
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
  getClusterStatus: id => toClusterStatusInfo(id, engine.status()),
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

function toReplicationStatusInfo(status: ReplicationStatus): ReplicationStatusInfo {
  return {
    role: status.role,
    writeForwarding: true,
    peers: status.peers.length,
    localSeq: BigInt(status.localSeq),
    replicationGroupId: status.coordinator?.groupId,
    primaryTerm: status.coordinator?.primaryTerm,
    currentPrimary: status.coordinator?.currentPrimary?.nodeId,
    coordinator: status.coordinator
      ? {
          connected: true,
          authority: status.coordinator.authority,
        }
      : undefined,
    controller: status.coordinator
      ? {
          state: status.coordinator.controllerState,
        }
      : undefined,
    inSyncReplicas: status.coordinator?.inSyncNodeIds.filter(
      inSyncNodeId => inSyncNodeId !== status.coordinator?.currentPrimary?.nodeId,
    ),
    laggingReplicas: status.coordinator?.votingDataBearingNodeIds.filter(
      votingNodeId => !status.coordinator?.inSyncNodeIds.includes(votingNodeId),
    ),
    syncState: status.syncState?.phase,
    readAvailability: readAvailability(status),
    writeAvailability: writeAvailability(status),
  }
}

function toClusterStatusInfo(id: string, status: ReplicationStatus): ClusterStatusInfo | null {
  if (id !== DATABASE_ID) return null
  const coordinatorState = status.coordinator
  return {
    databaseId: DATABASE_ID,
    replicationGroupId: coordinatorState?.groupId,
    role: status.role,
    currentPrimary: coordinatorState?.currentPrimary
      ? { ...coordinatorState.currentPrimary }
      : (coordinatorState?.currentPrimary ?? null),
    primaryTerm: coordinatorState?.primaryTerm,
    readEndpoints: coordinatorState?.inSyncNodeIds.map(inSyncNodeId => ({
      nodeId: inSyncNodeId,
      endpoint: httpEndpoints[inSyncNodeId] ?? '',
      readConcerns: ['local', 'majority'],
    })),
    health: clusterHealth(status),
  }
}

function clusterHealth(status: ReplicationStatus): ClusterStatusInfo['health'] {
  if (status.syncState?.phase === 'syncing' || status.syncState?.phase === 'catching-up') return 'syncing'
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(nodeId)) return 'repairing'
  if (coordinatorState.authority && writeAvailability(status) === 'unavailable') return 'failing_over'
  if (readAvailability(status) === 'unavailable' && writeAvailability(status) === 'unavailable') return 'unavailable'
  if (coordinatorState.faultedNodeIds.length > 0 || coordinatorState.drainingNodeIds.length > 0) return 'degraded'
  return 'healthy'
}

function readAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinatorState.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return coordinatorState.inSyncNodeIds.includes(status.nodeId) ? 'available' : 'unavailable'
}

function writeAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (!coordinatorState.authority) return 'unavailable'
  if (coordinatorState.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
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

function requireNodeId(): (typeof NODE_IDS)[number] {
  const value = requireEnv('NODE_ID')
  if (NODE_IDS.includes(value as (typeof NODE_IDS)[number])) {
    return value as (typeof NODE_IDS)[number]
  }
  throw new Error(`NODE_ID must be one of ${NODE_IDS.join(', ')}`)
}

function requireRole(): NodeRole {
  const value = requireEnv('INITIAL_ROLE')
  if (value === 'primary' || value === 'replica') return value
  throw new Error('INITIAL_ROLE must be primary or replica')
}

function requireClusterToken(): string {
  const value = requireEnv('SIRANNON_CLUSTER_TOKEN')
  console.log('Using SIRANNON_CLUSTER_TOKEN from the environment for cluster HTTP and WebSocket auth')
  return value
}

function requireEnv(name: string): string {
  const value = process.env[name]
  if (!value) {
    throw new Error(`${name} is required`)
  }
  return value
}

function requireCsv(name: string): string[] {
  const values = requireEnv(name)
    .split(',')
    .map(value => value.trim())
    .filter(value => value.length > 0)
  if (values.length === 0) {
    throw new Error(`${name} must contain at least one value`)
  }
  return values
}

function requirePort(name: string): number {
  const port = numberEnv(name, Number.NaN)
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`${name} must be a valid TCP port`)
  }
  return port
}

function numberEnv(name: string, fallback: number): number {
  const value = process.env[name]
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number`)
  }
  return parsed
}

function toJsonSafe(value: unknown): unknown {
  return JSON.parse(
    JSON.stringify(value, (_key, nestedValue) =>
      typeof nestedValue === 'bigint' ? nestedValue.toString() : nestedValue,
    ),
  )
}
