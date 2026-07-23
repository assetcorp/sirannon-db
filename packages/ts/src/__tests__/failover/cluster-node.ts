import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import { Sirannon } from '../../core/sirannon.js'
import type { ClusterStatusInfo, QueryOptions, ReplicationStatusInfo } from '../../core/types.js'
import { createEtcdCoordinator } from '../../replication/coordinator/etcd.js'
import { ReplicationEngine } from '../../replication/engine.js'
import { PrimaryReplicaTopology } from '../../replication/topology/primary-replica.js'
import type {
  ForwardedTransaction,
  ReplicationBatch,
  ReplicationStatus,
  SyncBatch,
  SyncRequest,
} from '../../replication/types.js'
import { createServer, type SirannonServer } from '../../server/index.js'
import { GrpcReplicationTransport } from '../../transport/grpc/index.js'
import type { FailoverNodeConfig, SerializedError } from './node-process.js'
import { serializeJson } from './node-process.js'

interface RequestMessage {
  id: number
  command: string
  payload: Record<string, unknown>
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS failover_items (
    id INTEGER PRIMARY KEY,
    owner TEXT NOT NULL,
    value INTEGER NOT NULL,
    note TEXT
  );

  CREATE TABLE IF NOT EXISTS failover_events (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES failover_items(id)
  )
`

const rawConfig = process.env.SIRANNON_FAILOVER_NODE_CONFIG
if (!rawConfig) {
  throw new Error('SIRANNON_FAILOVER_NODE_CONFIG is required')
}

const config = JSON.parse(rawConfig) as FailoverNodeConfig
const databaseId = config.groupId
const conn = await testDriver.open(config.dbPath)
await conn.exec('PRAGMA journal_mode = WAL')

const tracker = new ChangeTracker()
if (config.seedSchema) {
  await conn.exec(SCHEMA)
  await tracker.watch(conn, 'failover_items')
  await tracker.watch(conn, 'failover_events')
}

const sirannon = new Sirannon({ driver: testDriver })
const db = await sirannon.open(databaseId, config.dbPath)
const transport = new GrpcReplicationTransport({
  host: '127.0.0.1',
  port: config.grpcPort,
  tlsCert: config.certPath,
  tlsKey: config.keyPath,
  tlsCaCert: config.caCertPath,
})
const coordinator = createEtcdCoordinator({
  hosts: config.etcdHosts,
  keyPrefix: config.keyPrefix,
  allowInsecure: true,
  dialTimeoutMs: 1_000,
  defaultCallTimeoutMs: 1_000,
})

const engine = new ReplicationEngine(db, conn, {
  nodeId: config.nodeId,
  topology: new PrimaryReplicaTopology(config.initialRole),
  transport,
  transportConfig: transportConfig(),
  batchIntervalMs: 25,
  batchSize: 100,
  maxBatchChanges: 1_000,
  maxPendingBatches: 4,
  ackTimeoutMs: 1_500,
  initialSync: true,
  changeTracker: tracker,
  snapshotConnectionFactory: () => testDriver.open(config.dbPath, { readonly: true }),
  syncBatchSize: 100,
  syncAckTimeoutMs: 5_000,
  catchUpDeadlineMs: 8_000,
  maxSyncLagBeforeReady: 0,
  writeForwarding: true,
  coordinator: {
    clusterId: config.clusterId,
    groupId: config.groupId,
    endpoint: config.httpEndpoints[config.nodeId],
    coordinator,
    votingDataBearingNodeIds: config.votingDataBearingNodeIds,
    sessionTtlMs: config.sessionTtlMs,
    controller: {
      enabled: true,
      leaseTtlMs: config.controllerLeaseTtlMs,
      tickIntervalMs: config.controllerTickIntervalMs,
    },
    compatibility: config.compatibility,
  },
})

let server: SirannonServer | null = null

const recentErrors: unknown[] = []
engine.on('replication-error', event => {
  recentErrors.push(serializeJson(event))
  if (recentErrors.length > 30) {
    recentErrors.shift()
  }
})

await engine.start()
server = createServer(sirannon, {
  host: '127.0.0.1',
  port: config.httpPort,
  resolveExecutionTarget: id => (id === databaseId ? engine : null),
  getReplicationStatus: () => toReplicationStatusInfo(engine.status()),
  getClusterStatus: id => toClusterStatusInfo(id, engine.status()),
})
await server.listen()
process.send?.({ type: 'ready', nodeId: config.nodeId })

process.on('message', message => {
  handleMessage(message as RequestMessage).catch(err => {
    const request = message as RequestMessage
    sendResponse(request.id, false, undefined, serializeError(err))
  })
})

process.on('SIGTERM', () => {
  shutdown().finally(() => {
    process.exit(0)
  })
})

process.on('SIGINT', () => {
  shutdown().finally(() => {
    process.exit(0)
  })
})

async function handleMessage(message: RequestMessage): Promise<void> {
  const { id, command, payload } = message
  try {
    if (command === 'execute') {
      const result = await engine.execute(
        stringPayload(payload, 'sql'),
        optionalArrayPayload(payload, 'params'),
        payload.options as QueryOptions | undefined,
      )
      sendResponse(id, true, serializeJson(result))
      return
    }
    if (command === 'executeBatch') {
      const result = await engine.executeBatch(
        stringPayload(payload, 'sql'),
        arrayPayload(payload, 'paramsBatch') as unknown[][],
        payload.options as QueryOptions | undefined,
      )
      sendResponse(id, true, serializeJson(result))
      return
    }
    if (command === 'localWriteProbe') {
      const result = await engine.transaction(
        async tx =>
          tx.execute('INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)', [
            numberPayload(payload, 'id'),
            config.nodeId,
            numberPayload(payload, 'id'),
            stringPayload(payload, 'note'),
          ]),
        { writeConcern: { level: 'local' } },
      )
      sendResponse(id, true, serializeJson(result))
      return
    }
    if (command === 'query') {
      const result = await engine.query(
        stringPayload(payload, 'sql'),
        optionalArrayPayload(payload, 'params'),
        payload.options as QueryOptions | undefined,
      )
      sendResponse(id, true, serializeJson(result))
      return
    }
    if (command === 'status') {
      sendResponse(id, true, serializeJson({ ...engine.status(), recentErrors }))
      return
    }
    if (command === 'reconnectTransport') {
      await transport.disconnect()
      await transport.connect(config.nodeId, transportConfig())
      sendResponse(id, true, null)
      return
    }
    if (command === 'sendRawBatch') {
      await transport.send(stringPayload(payload, 'peerId'), parseBatch(payload.batch))
      sendResponse(id, true, null)
      return
    }
    if (command === 'requestRawSync') {
      await transport.requestSync(stringPayload(payload, 'peerId'), parseSyncRequest(payload.request))
      sendResponse(id, true, null)
      return
    }
    if (command === 'sendRawSyncBatch') {
      await transport.sendSyncBatch(stringPayload(payload, 'peerId'), parseSyncBatch(payload.batch))
      sendResponse(id, true, null)
      return
    }
    if (command === 'sendRawForward') {
      await transport.forward(stringPayload(payload, 'peerId'), parseForwardRequest(payload.request))
      sendResponse(id, true, null)
      return
    }
    if (command === 'shutdown') {
      await shutdown()
      sendResponse(id, true, null)
      process.exit(0)
    }
    throw new Error(`Unknown command '${command}'`)
  } catch (err: unknown) {
    sendResponse(id, false, undefined, serializeError(err))
  }
}

async function shutdown(): Promise<void> {
  await server?.close().catch(() => undefined)
  server = null
  await engine.stop().catch(() => undefined)
  await coordinator.close().catch(() => undefined)
  if (!db.closed) {
    await db.close().catch(() => undefined)
  }
  await sirannon.shutdown().catch(() => undefined)
  await conn.close().catch(() => undefined)
}

function transportConfig() {
  return {
    localRole: config.initialRole,
    endpoints: config.endpoints,
    groupId: config.groupId,
    protocolVersion: config.compatibility.protocolVersion,
  }
}

function sendResponse(id: number, ok: boolean, result?: unknown, error?: SerializedError): void {
  process.send?.({
    type: 'response',
    id,
    ok,
    result,
    error,
  })
}

function serializeError(err: unknown): SerializedError {
  if (!(err instanceof Error)) {
    return {
      name: 'Error',
      message: String(err),
    }
  }
  const withCode = err as Error & {
    code?: string
    details?: Record<string, unknown>
  }
  return {
    name: err.name,
    message: err.message,
    code: withCode.code,
    details: withCode.details,
  }
}

function stringPayload(payload: Record<string, unknown>, key: string): string {
  const value = payload[key]
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`Payload field '${key}' must be a non-empty string`)
  }
  return value
}

function optionalArrayPayload(payload: Record<string, unknown>, key: string): unknown[] | undefined {
  const value = payload[key]
  if (value === undefined) return undefined
  if (!Array.isArray(value)) {
    throw new Error(`Payload field '${key}' must be an array when present`)
  }
  return value
}

function numberPayload(payload: Record<string, unknown>, key: string): number {
  const value = payload[key]
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new Error(`Payload field '${key}' must be a safe integer`)
  }
  return value
}

function arrayPayload(payload: Record<string, unknown>, key: string): unknown[] {
  const value = optionalArrayPayload(payload, key)
  if (!value) {
    throw new Error(`Payload field '${key}' is required`)
  }
  return value
}

function parseBatch(value: unknown): ReplicationBatch {
  const batch = objectPayload(value, 'batch') as unknown as ReplicationBatch & {
    fromSeq: string
    toSeq: string
    primaryTerm?: string
  }
  return {
    ...batch,
    fromSeq: BigInt(batch.fromSeq),
    toSeq: BigInt(batch.toSeq),
    primaryTerm: batch.primaryTerm === undefined ? undefined : BigInt(batch.primaryTerm),
  }
}

function parseSyncRequest(value: unknown): SyncRequest {
  const request = objectPayload(value, 'request') as unknown as SyncRequest & {
    primaryTerm?: string
  }
  return {
    ...request,
    primaryTerm: request.primaryTerm === undefined ? undefined : BigInt(request.primaryTerm),
  }
}

function parseSyncBatch(value: unknown): SyncBatch {
  const batch = objectPayload(value, 'sync batch') as unknown as SyncBatch & {
    primaryTerm?: string
  }
  return {
    ...batch,
    primaryTerm: batch.primaryTerm === undefined ? undefined : BigInt(batch.primaryTerm),
  }
}

function parseForwardRequest(value: unknown): ForwardedTransaction {
  const request = objectPayload(value, 'forward request') as unknown as ForwardedTransaction & {
    primaryTerm?: string
  }
  return {
    ...request,
    primaryTerm: request.primaryTerm === undefined ? undefined : BigInt(request.primaryTerm),
  }
}

function objectPayload(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Payload field '${name}' must be an object`)
  }
  return value as Record<string, unknown>
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
      nodeId => nodeId !== status.coordinator?.currentPrimary?.nodeId,
    ),
    laggingReplicas: status.coordinator?.votingDataBearingNodeIds.filter(
      nodeId => !status.coordinator?.inSyncNodeIds.includes(nodeId),
    ),
    syncState: status.syncState?.phase,
    readAvailability: readAvailability(status),
    writeAvailability: writeAvailability(status),
  }
}

function toClusterStatusInfo(id: string, status: ReplicationStatus): ClusterStatusInfo | null {
  if (id !== databaseId) return null
  const coordinator = status.coordinator
  return {
    databaseId,
    replicationGroupId: coordinator?.groupId,
    role: status.role,
    currentPrimary: coordinator?.currentPrimary
      ? { ...coordinator.currentPrimary }
      : (coordinator?.currentPrimary ?? null),
    primaryTerm: coordinator?.primaryTerm,
    readEndpoints: coordinator?.inSyncNodeIds.map(nodeId => ({
      nodeId,
      endpoint: config.httpEndpoints[nodeId] ?? '',
      readConcerns: ['local', 'majority'],
    })),
    health: clusterHealth(status),
  }
}

function clusterHealth(status: ReplicationStatus): ClusterStatusInfo['health'] {
  if (status.syncState?.phase === 'syncing' || status.syncState?.phase === 'catching-up') return 'syncing'
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(config.nodeId)) return 'repairing'
  if (coordinator.authority && writeAvailability(status) === 'unavailable') return 'failing_over'
  if (readAvailability(status) === 'unavailable' && writeAvailability(status) === 'unavailable') return 'unavailable'
  if (coordinator.faultedNodeIds.length > 0 || coordinator.drainingNodeIds.length > 0) return 'degraded'
  return 'healthy'
}

function readAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return coordinator.inSyncNodeIds.includes(status.nodeId) ? 'available' : 'unavailable'
}

function writeAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (!coordinator.authority) return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
}
