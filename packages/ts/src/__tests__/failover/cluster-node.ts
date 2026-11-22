import type { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import type { LiveQuery } from '../../core/live/types.js'
import type { DatabaseOptions, QueryOptions } from '../../core/types.js'
import type { EtcdClusterCoordinator } from '../../replication/coordinator/etcd.js'
import type { ReplicationEngine as ReplicationEngineType } from '../../replication/engine.js'
import type { SirannonServer } from '../../server/index.js'
import type { GrpcReplicationTransport as GrpcReplicationTransportType } from '../../transport/grpc/index.js'
import { createFailoverCoordinator, groupReadHold, inSyncUpdateHold } from './cluster-node-coordinator.js'
import { numberPayload, optionalArrayPayload, serializeError, stringPayload } from './cluster-node-payloads.js'
import { handleTransportCommand, proxiedTransportClass } from './cluster-node-transport.js'
import { directoryDestination } from './directory-destination.js'
import type { FailoverNodeConfig, SerializedError } from './node-process.js'
import { serializeJson } from './node-process.js'

interface RequestMessage {
  id: number
  command: string
  payload: Record<string, unknown>
}

interface RunningNode {
  engine: ReplicationEngineType
  database: Database
  transport: GrpcReplicationTransportType
  shutdown: () => Promise<void>
}

type ClusterNodeLibrary = typeof import('./cluster-node-library.js')

const rawConfig = process.env.SIRANNON_FAILOVER_NODE_CONFIG
if (!rawConfig) {
  throw new Error('SIRANNON_FAILOVER_NODE_CONFIG is required')
}
const config = JSON.parse(rawConfig) as FailoverNodeConfig
const library: ClusterNodeLibrary = config.publishedRelease
  ? ((await import('./cluster-node-library-published.js')) as unknown as ClusterNodeLibrary)
  : await import('./cluster-node-library.js')
const {
  ChangeTracker,
  Sirannon,
  coordinatorBackupGroup,
  createEtcdCoordinator,
  createServer,
  GrpcReplicationTransport,
  PrimaryReplicaTopology,
  ReplicationEngine,
  testDriver,
  toClusterStatusInfo,
  toReplicationStatusInfo,
} = library

const ProxiedGrpcTransport = proxiedTransportClass(GrpcReplicationTransport, config.peerProxyEndpoints)

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

const databaseId = config.groupId
const recentErrors: unknown[] = []

let running: RunningNode | null = null
let liveItems: LiveQuery<{ id: number }> | null = null
try {
  running = await startClusterNode()
  process.send?.({ type: 'ready', nodeId: config.nodeId })
} catch (err: unknown) {
  process.send?.({ type: 'startup-failed', error: serializeError(err) }, () => {
    process.exit(1)
  })
}

process.on('message', message => {
  const request = message as RequestMessage
  handleMessage(request).catch(err => {
    sendResponse(request.id, false, undefined, serializeError(err))
  })
})

process.on('SIGTERM', () => {
  void stopAndExit()
})

process.on('SIGINT', () => {
  void stopAndExit()
})

function databaseOptions(coordinator: EtcdClusterCoordinator): DatabaseOptions | undefined {
  if (!config.backupDirectory) return undefined
  return {
    backups: {
      destination: directoryDestination(config.backupDirectory),
      intervalMs: 0,
      namePrefix: `backup-${config.nodeId}`,
      replicationGroup: coordinatorBackupGroup({
        coordinator,
        clusterId: config.clusterId,
        groupId: config.groupId,
        nodeId: config.nodeId,
      }),
      onError: err => {
        recordError({ error: serializeError(err), operation: 'backup-cycle' })
      },
    },
  }
}

async function startClusterNode(): Promise<RunningNode> {
  const coordinator = createFailoverCoordinator(createEtcdCoordinator, config)
  const conn: SQLiteConnection = await testDriver.open(config.dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')

  const tracker = new ChangeTracker()
  if (config.seedSchema) {
    await conn.exec(SCHEMA)
    await tracker.watch(conn, 'failover_items')
    await tracker.watch(conn, 'failover_events')
  }

  const sirannon = new Sirannon({ driver: testDriver })
  const db = await sirannon.open(databaseId, config.dbPath, databaseOptions(coordinator))
  const transport = new ProxiedGrpcTransport({
    host: '127.0.0.1',
    port: config.grpcPort,
    tlsCert: config.certPath,
    tlsKey: config.keyPath,
    tlsCaCert: config.caCertPath,
  })

  const engine = new ReplicationEngine(db, conn, {
    ...(config.stableNodeId ? { nodeId: config.nodeId } : {}),
    topology: new PrimaryReplicaTopology(config.initialRole),
    transport,
    transportConfig: transportConfig(),
    batchIntervalMs: 25,
    batchSize: 100,
    maxBatchChanges: 1_000,
    maxPendingBatches: 4,
    ackTimeoutMs: 1_500,
    initialSync: config.initialSync ?? true,
    changeTracker: tracker,
    snapshotConnectionFactory: () => testDriver.open(config.dbPath, { readonly: true }),
    syncBatchSize: 100,
    syncAckTimeoutMs: 5_000,
    catchUpDeadlineMs: 8_000,
    maxSyncLagBeforeReady: 0,
    writeForwarding: config.writeForwarding,
    coordinator: {
      clusterId: config.clusterId,
      groupId: config.groupId,
      endpoint: config.httpEndpoint,
      replicationEndpoint: `127.0.0.1:${config.grpcPort}`,
      coordinator,
      votingDataBearingNodeIds: config.votingDataBearingNodeIds,
      sessionTtlMs: config.sessionTtlMs,
      maxReplicaLagMs: config.maxReplicaLagMs,
      controller: {
        enabled: config.controllerEnabled,
        leaseTtlMs: config.controllerLeaseTtlMs,
        tickIntervalMs: config.controllerTickIntervalMs,
      },
      compatibility: config.compatibility,
    },
  })

  engine.on('replication-error', event => {
    recordError(
      serializeJson({
        error: serializeError(event.error),
        operation: event.operation,
        peerId: event.peerId,
        recoverable: event.recoverable,
      }),
    )
  })

  let server: SirannonServer | null = null
  const shutdown = async (): Promise<void> => {
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

  try {
    await engine.start()
    server = createServer(sirannon, {
      host: '127.0.0.1',
      port: config.httpPort,
      acceptSql: true,
      resolveExecutionTarget: id => (id === databaseId ? engine : null),
      getReplicationStatus: () => toReplicationStatusInfo(engine.status()),
      getClusterStatus: id => (id === databaseId ? toClusterStatusInfo(engine.status(), { databaseId }) : null),
      authorizeClusterStatus: () => true,
    })
    await server.listen()
  } catch (err: unknown) {
    await shutdown()
    throw err
  }

  return { engine, database: db, transport, shutdown }
}

function recordError(event: unknown): void {
  recentErrors.push(event)
  if (recentErrors.length > 50) {
    recentErrors.shift()
  }
}

function requireRunning(): RunningNode {
  if (!running) throw new Error(`failover node ${config.nodeId} did not start`)
  return running
}

async function handleMessage(message: RequestMessage): Promise<void> {
  const { id, command, payload } = message
  try {
    const node = requireRunning()
    const { engine, transport } = node
    if (command === 'execute') {
      const result = await engine.execute(
        stringPayload(payload, 'sql'),
        optionalArrayPayload(payload, 'params'),
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
    if (command === 'openLiveItems') {
      liveItems ??= await node.database.live<{ id: number }>('SELECT id FROM failover_items ORDER BY id')
      sendResponse(id, true, null)
      return
    }
    if (command === 'liveItemIds') {
      const state = liveItems?.getState()
      sendResponse(id, true, state?.status === 'ready' ? state.rows.map(row => Number(row.id)) : null)
      return
    }
    if (command === 'status') {
      sendResponse(id, true, serializeJson({ ...engine.status(), recentErrors }))
      return
    }
    if (command === 'holdInSyncUpdates') {
      const removesNodeId = payload.removesNodeId
      inSyncUpdateHold.hold({
        ...(typeof removesNodeId === 'string' ? { removesNodeId } : {}),
        admissions: payload.admissions === true,
        ...(typeof payload.admitsNodeId === 'string' ? { admitsNodeId: payload.admitsNodeId } : {}),
      })
      sendResponse(id, true, null)
      return
    }
    if (command === 'releaseInSyncUpdates') {
      inSyncUpdateHold.release()
      sendResponse(id, true, null)
      return
    }
    if (command === 'heldInSyncUpdates') {
      sendResponse(id, true, inSyncUpdateHold.held())
      return
    }
    if (command === 'groupReads') {
      sendResponse(id, true, await groupReadHold.configure(stringPayload(payload, 'action')))
      return
    }
    if (command === 'reconnectTransport') {
      await transport.disconnect()
      await transport.connect(config.nodeId, transportConfig())
      sendResponse(id, true, null)
      return
    }
    const transportResult = await handleTransportCommand(transport, command, payload)
    if (transportResult !== undefined) {
      sendResponse(id, true, transportResult)
      return
    }
    if (command === 'shutdown') {
      await node.shutdown()
      sendResponse(id, true, null)
      process.exit(0)
    }
    throw new Error(`Unknown command '${command}'`)
  } catch (err: unknown) {
    sendResponse(id, false, undefined, serializeError(err))
  }
}

async function stopAndExit(): Promise<void> {
  await running?.shutdown().catch(() => undefined)
  process.exit(0)
}

function transportConfig() {
  return {
    localRole: config.initialRole,
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
