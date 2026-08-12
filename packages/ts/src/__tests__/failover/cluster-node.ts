import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import { Sirannon } from '../../core/sirannon.js'
import type { QueryOptions } from '../../core/types.js'
import { toClusterStatusInfo, toReplicationStatusInfo } from '../../replication/cluster-status.js'
import { createEtcdCoordinator } from '../../replication/coordinator/etcd.js'
import { ReplicationEngine } from '../../replication/engine.js'
import { PrimaryReplicaTopology } from '../../replication/topology/primary-replica.js'
import { createServer, type SirannonServer } from '../../server/index.js'
import { GrpcReplicationTransport } from '../../transport/grpc/index.js'
import {
  arrayPayload,
  numberPayload,
  optionalArrayPayload,
  parseBatch,
  parseForwardRequest,
  parseSyncBatch,
  parseSyncRequest,
  serializeError,
  stringPayload,
} from './cluster-node-payloads.js'

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
  acceptSql: true,
  resolveExecutionTarget: id => (id === databaseId ? engine : null),
  getReplicationStatus: () => toReplicationStatusInfo(engine.status()),
  getClusterStatus: id =>
    id === databaseId ? toClusterStatusInfo(engine.status(), { databaseId, endpoints: config.httpEndpoints }) : null,
  authorizeClusterStatus: () => true,
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
