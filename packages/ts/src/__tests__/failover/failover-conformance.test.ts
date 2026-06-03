import { createHash } from 'node:crypto'
import { existsSync, mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../client/index.js'
import { createEtcdCoordinator, type EtcdClusterCoordinator } from '../../replication/coordinator/etcd.js'
import { canonicaliseForChecksum } from '../../replication/log.js'
import type {
  ForwardedTransaction,
  ReplicationBatch,
  ReplicationChange,
  SyncBatch,
  SyncRequest,
} from '../../replication/types.js'
import { createMtlsCerts, type MtlsCerts } from '../e2e/lib/certs.js'
import {
  allocatePorts,
  cleanupDockerResources,
  type EtcdClusterHandle,
  ensureFailoverImages,
  failoverRunPrefix,
  startEtcdCluster,
  startToxiproxyContainer,
  type ToxiproxyContainerHandle,
} from './docker.js'
import { type FailoverNodeConfig, FailoverNodeProcess, type SerializedError } from './node-process.js'
import { ToxiproxyClient } from './toxiproxy.js'

const NODE_A = 'failover-node-a'
const NODE_B = 'failover-node-b'
const NODE_C = 'failover-node-c'
const GROUP_ID = 'orders'
const CLUSTER_ID = 'cluster-a'
const NODE_IDS = [NODE_A, NODE_B, NODE_C] as const
const COMPATIBILITY = {
  packageVersion: '1.0.0',
  specVersion: '1.0.0',
  protocolVersion: '1.0.0',
}

interface GateEnvironment {
  runPrefix: string
  tempDir: string
  certs: MtlsCerts
  etcd: EtcdClusterHandle
  toxiproxyContainer: ToxiproxyContainerHandle
  toxiproxy: ToxiproxyClient
  coordinator: EtcdClusterCoordinator
  grpcPorts: Map<string, number>
  httpPorts: Map<string, number>
  etcdProxyPorts: Map<string, number[]>
  grpcProxyPorts: Map<string, Map<string, number>>
  nodes: Map<string, FailoverNodeProcess>
}

interface FailoverStatus {
  nodeId: string
  role: string
  peers: Array<{ nodeId: string; lastAckedSeq: string; connected: boolean }>
  localSeq: string
  replicating: boolean
  syncState?: {
    phase: string
    sourcePeerId: string | null
  }
  coordinator?: {
    currentPrimary: { nodeId: string; endpoint?: string } | null
    primaryTerm: string
    inSyncNodeIds: string[]
    drainingNodeIds: string[]
    repairingNodeIds: string[]
    faultedNodeIds: string[]
    votingDataBearingNodeIds: string[]
    authority: boolean
    controllerState: 'disabled' | 'standby' | 'active' | 'lost'
  }
  recentErrors?: Array<{
    error?: SerializedError
    operation?: string
    peerId?: string
    recoverable?: boolean
  }>
}

interface ReadinessResponse {
  status: string
  replication?: {
    primaryTerm?: string
    currentPrimary?: string
    readAvailability?: 'available' | 'unavailable'
    writeAvailability?: 'available' | 'unavailable'
    syncState?: string
    coordinator?: {
      authority: boolean
    }
    controller?: {
      state: string
    }
  }
}

let env: GateEnvironment | null = null
let writeProbeId = 1_000

describe('batch two failover conformance gate', () => {
  afterEach(async () => {
    if (env) {
      await cleanupEnvironment(env)
      env = null
    }
  })

  it('proves coordinator-mode failover invariants with real etcd, gRPC, durable nodes, and TCP link faults', async () => {
    env = await startEnvironment()

    const nodeA = await startNode(env, NODE_A, 'primary', [], true, true)
    await waitForNodeReady(nodeA)
    const nodeB = await startNode(env, NODE_B, 'replica', [NODE_A, NODE_C], false, false)
    await waitForNodeReady(nodeB)
    const nodeC = await startNode(env, NODE_C, 'replica', [NODE_A, NODE_B], false, false)
    await waitForNodeReady(nodeC)
    await nodeB.reconnectTransport()
    await waitForPeerConnected(nodeB, NODE_C, 10_000)

    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 30_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, '1')

    await executeMajority(nodeA, 1, 'majority-before-failover')
    await waitForRows(env, [NODE_A, NODE_B, NODE_C], 1, 30_000)
    await executePublicHttp(env, NODE_A, "UPDATE failover_items SET note = 'public-http' WHERE id = 1")
    await waitForItemNote(env, [NODE_A, NODE_B, NODE_C], 1, 'public-http', 30_000)
    await assertHealth(env, NODE_A, '1', 'available', 'available')

    await setEtcdLink(env, NODE_A, false)
    await expectRejectsWith(
      nodeA.execute("INSERT INTO failover_items (id, owner, value, note) VALUES (99, 'a', 99, 'stale')"),
      ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY'],
    )
    await expectPublicHttpRejectsWith(
      env,
      NODE_A,
      "INSERT INTO failover_items (id, owner, value, note) VALUES (98, 'a', 98, 'public-stale')",
      ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY'],
    )
    await assertItemAbsent(env, [NODE_A, NODE_B, NODE_C], 99, 10_000)
    await assertItemAbsent(env, [NODE_A, NODE_B, NODE_C], 98, 10_000)

    nodeA.kill()
    env.nodes.delete(NODE_A)
    const promotedNodeId = await waitForAnyCurrentPrimary(env, [NODE_B, NODE_C], 30_000)
    const promotedNode = requireNode(env, promotedNodeId)
    const forwardingNodeId = promotedNodeId === NODE_B ? NODE_C : NODE_B
    const forwardingNode = requireNode(env, forwardingNodeId)
    await expectExactlyOneWritablePrimary(env, promotedNodeId, '2')
    await assertItemVisible(promotedNode, 1)
    await forwardingNode.reconnectTransport()
    await waitForPeerConnected(forwardingNode, promotedNodeId, 10_000)

    await expectStaleBatchRejectedWithoutMutation(
      env,
      forwardingNode,
      promotedNode,
      promotedNodeId,
      forwardingNodeId,
      70,
    )
    await expectStaleForwardRejectedWithoutMutation(env, forwardingNode, promotedNodeId, 71)
    await expectStaleSyncRequestRejectedWithoutMutation(env, forwardingNode, promotedNodeId, forwardingNodeId, 72)
    await expectStaleSyncBatchRejectedWithoutMutation(env, forwardingNode, promotedNodeId, 73)

    await forwardingNode.execute(
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (10, 1, 'forwarded', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 10, 20_000)
    await executePublicClient(
      env,
      forwardingNodeId,
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (11, 1, 'public-client', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 11, 20_000)
    await executePublicWebSocket(
      env,
      forwardingNodeId,
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (12, 1, 'public-ws', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 12, 20_000)
    await assertItemVisible(forwardingNode, 1)

    await healNodeSirannonLinks(env, NODE_A)
    const restartedA = await startNode(env, NODE_A, 'primary', [NODE_B, NODE_C], false, false)
    await waitForNodeReady(restartedA, 30_000)
    await waitForInSyncSet(env, [NODE_A, promotedNodeId, forwardingNodeId], 30_000)
    await waitForEvent(env, [NODE_A], 10, 20_000)
    await assertHealth(env, promotedNodeId, '2', 'available', 'available')

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: promotedNodeId,
      draining: true,
    })
    await waitForCurrentPrimary(env, NODE_A, 30_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, '3')
    await assertHealth(env, NODE_A, '3', 'available', 'available')

    const safeReplicaAfterMaintenance = promotedNodeId === NODE_B ? NODE_C : NODE_B
    await runSeededSoak(env, NODE_A, [NODE_A, safeReplicaAfterMaintenance])
    await restartReplicaThroughRepair(env, safeReplicaAfterMaintenance, NODE_A)
    await expectResourceUseBelow(env, 256 * 1024 * 1024)

    await env.coordinator.updateInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      inSyncNodeIds: [NODE_A, safeReplicaAfterMaintenance],
    })
    restartedA.kill()
    env.nodes.delete(NODE_A)
    await waitForCurrentPrimary(env, safeReplicaAfterMaintenance, 30_000)
    await expectExactlyOneWritablePrimary(env, safeReplicaAfterMaintenance, '4')
    await waitForRows(env, [safeReplicaAfterMaintenance], 13, 30_000)

    await env.coordinator.setReplicationGroupState({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      votingDataBearingNodeIds: [...NODE_IDS],
      currentPrimary: {
        nodeId: safeReplicaAfterMaintenance,
        endpoint: httpEndpointFor(env, safeReplicaAfterMaintenance),
      },
      primaryTerm: 5n,
      inSyncNodeIds: [safeReplicaAfterMaintenance],
      compatibility: {
        packageVersion: '2.0.0',
        specVersion: '2.0.0',
        protocolVersion: '2.0.0',
      },
    })
    await waitForCurrentPrimary(env, safeReplicaAfterMaintenance, 10_000)
    const finalPrimary = requireNode(env, safeReplicaAfterMaintenance)
    await expectRejectsWith(
      finalPrimary.execute(
        "INSERT INTO failover_items (id, owner, value, note) VALUES (90, 'c', 90, 'incompatible')",
        undefined,
        {
          writeConcern: { level: 'local' },
        },
      ),
      ['PROTOCOL_VERSION_MISMATCH'],
    )

    finalPrimary.kill()
    env.nodes.delete(safeReplicaAfterMaintenance)
    const survivors = [...env.nodes.keys()]
    await waitForNoSafePrimary(env, survivors, 30_000)
    for (const survivorId of survivors) {
      await expectRejectsWith(
        requireNode(env, survivorId).execute(
          "INSERT INTO failover_items (id, owner, value, note) VALUES (80, 'survivor', 80, 'unsafe')",
        ),
        ['TOPOLOGY_ERROR', 'STALE_PRIMARY', 'NO_SAFE_PRIMARY', 'PROTOCOL_VERSION_MISMATCH', 'SYNC_ERROR'],
      )
    }
  })
})

async function startEnvironment(): Promise<GateEnvironment> {
  await ensureFailoverImages()
  const runPrefix = failoverRunPrefix()
  const tempDir = mkdtempSync(join(tmpdir(), `${runPrefix}-`))
  const certs = await createMtlsCerts(NODE_IDS)
  const etcd = await startEtcdCluster(runPrefix)
  const grpcPorts = new Map<string, number>(zip(NODE_IDS, await allocatePorts(NODE_IDS.length)))
  const httpPorts = new Map<string, number>(zip(NODE_IDS, await allocatePorts(NODE_IDS.length)))
  const etcdProxyPorts = await allocateEtcdProxyPorts()
  const grpcProxyPorts = await allocateGrpcProxyPorts()
  const allProxyPorts = [
    ...Array.from(etcdProxyPorts.values()).flat(),
    ...Array.from(grpcProxyPorts.values()).flatMap(value => Array.from(value.values())),
  ]
  const toxiproxyContainer = await startToxiproxyContainer(runPrefix, allProxyPorts)
  const toxiproxy = new ToxiproxyClient(`http://127.0.0.1:${toxiproxyContainer.apiPort}`)

  for (const nodeId of NODE_IDS) {
    const ports = etcdProxyPorts.get(nodeId)
    if (!ports) throw new Error(`Missing etcd proxy ports for ${nodeId}`)
    for (let index = 0; index < ports.length; index++) {
      const upstreamPort = etcd.clientPorts[index]
      const listenPort = ports[index]
      if (upstreamPort === undefined || listenPort === undefined) {
        throw new Error('Invalid etcd proxy allocation')
      }
      await toxiproxy.createProxy({
        name: etcdProxyName(nodeId, index),
        listenPort,
        upstreamPort,
      })
    }
  }

  for (const fromNodeId of NODE_IDS) {
    const targetPorts = grpcProxyPorts.get(fromNodeId)
    if (!targetPorts) throw new Error(`Missing gRPC proxy ports for ${fromNodeId}`)
    for (const [toNodeId, listenPort] of targetPorts) {
      const upstreamPort = grpcPorts.get(toNodeId)
      if (upstreamPort === undefined) {
        throw new Error(`Missing gRPC port for ${toNodeId}`)
      }
      await toxiproxy.createProxy({
        name: grpcProxyName(fromNodeId, toNodeId),
        listenPort,
        upstreamPort,
      })
    }
  }

  const coordinator = createEtcdCoordinator({
    hosts: etcd.endpoints,
    keyPrefix: runPrefix,
    allowInsecure: true,
    dialTimeoutMs: 1_000,
    defaultCallTimeoutMs: 1_000,
  })

  return {
    runPrefix,
    tempDir,
    certs,
    etcd,
    toxiproxyContainer,
    toxiproxy,
    coordinator,
    grpcPorts,
    httpPorts,
    etcdProxyPorts,
    grpcProxyPorts,
    nodes: new Map(),
  }
}

async function startNode(
  environment: GateEnvironment,
  nodeId: string,
  role: 'primary' | 'replica',
  endpointTargets: string[],
  seedSchema: boolean,
  createsGroup: boolean,
): Promise<FailoverNodeProcess> {
  const cert = environment.certs.certForNode(nodeId)
  const grpcPort = environment.grpcPorts.get(nodeId)
  if (grpcPort === undefined) throw new Error(`Missing gRPC port for ${nodeId}`)

  const config: FailoverNodeConfig = {
    nodeId,
    dbPath: join(environment.tempDir, `${nodeId}.db`),
    grpcPort,
    httpPort: httpPortFor(environment, nodeId),
    certPath: cert.certPath,
    keyPath: cert.keyPath,
    caCertPath: environment.certs.caCertPath,
    initialRole: role,
    endpoints: endpointTargets.map(target => endpointFor(environment, nodeId, target)),
    httpEndpoints: Object.fromEntries(NODE_IDS.map(target => [target, httpEndpointFor(environment, target)])),
    etcdHosts: etcdEndpointsFor(environment, nodeId),
    keyPrefix: environment.runPrefix,
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    votingDataBearingNodeIds: createsGroup ? [...NODE_IDS] : undefined,
    seedSchema,
    sessionTtlMs: 1_500,
    controllerLeaseTtlMs: 1_500,
    controllerTickIntervalMs: 200,
    compatibility: COMPATIBILITY,
  }

  const node = new FailoverNodeProcess(config)
  environment.nodes.set(nodeId, node)
  await node.ready()
  return node
}

async function cleanupEnvironment(environment: GateEnvironment): Promise<void> {
  for (const node of environment.nodes.values()) {
    await node.shutdown().catch(() => {
      node.kill()
    })
  }
  await environment.coordinator.close().catch(() => undefined)
  environment.certs.cleanup()
  rmSync(environment.tempDir, { recursive: true, force: true })
  await cleanupDockerResources({
    containers: [...environment.etcd.containerNames, environment.toxiproxyContainer.containerName],
    networks: [environment.etcd.networkName],
  })
}

async function restartReplicaThroughRepair(
  environment: GateEnvironment,
  nodeId: string,
  currentPrimaryNodeId: string,
): Promise<FailoverNodeProcess> {
  await environment.coordinator.updateNodeMaintenance({
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    nodeId,
    draining: false,
    repairing: true,
    faulted: false,
  })
  const existing = environment.nodes.get(nodeId)
  if (existing) {
    existing.kill()
    environment.nodes.delete(nodeId)
    await waitForCondition(async () => {
      const session = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId)
      return session === null
    }, 5_000)
  }
  await healNodeSirannonLinks(environment, nodeId)
  const restarted = await startNode(
    environment,
    nodeId,
    'replica',
    NODE_IDS.filter(targetNodeId => targetNodeId !== nodeId),
    false,
    false,
  )
  await waitForNodeReady(restarted, 30_000)
  await waitForPeerConnected(restarted, currentPrimaryNodeId, 10_000)
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return (latestState as { inSyncNodeIds?: string[] } | null)?.inSyncNodeIds?.includes(nodeId) ?? false
    }, 30_000)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for repaired replica ${nodeId} to re-enter in-sync set: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\ndiagnostics=${JSON.stringify(diagnostics, jsonReplacer, 2)}`,
    )
  }
  return restarted
}

async function allocateEtcdProxyPorts(): Promise<Map<string, number[]>> {
  const result = new Map<string, number[]>()
  for (const nodeId of NODE_IDS) {
    result.set(nodeId, await allocatePorts(3))
  }
  return result
}

async function allocateGrpcProxyPorts(): Promise<Map<string, Map<string, number>>> {
  const result = new Map<string, Map<string, number>>()
  for (const fromNodeId of NODE_IDS) {
    const targets = NODE_IDS.filter(nodeId => nodeId !== fromNodeId)
    const ports = await allocatePorts(targets.length)
    result.set(fromNodeId, new Map(zip(targets, ports)))
  }
  return result
}

async function waitForNodeReady(node: FailoverNodeProcess, timeoutMs = 20_000): Promise<void> {
  let latestStatus: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latestStatus = await statusOf(node)
      return latestStatus.syncState?.phase === 'ready'
    }, timeoutMs)
  } catch (err: unknown) {
    const statusText = JSON.stringify(latestStatus, null, 2)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${node.config.nodeId} did not become ready: ${message}\nstatus=${statusText}\n${node.logs()}`)
  }
}

async function waitForPeerConnected(node: FailoverNodeProcess, peerId: string, timeoutMs: number): Promise<void> {
  let latestStatus: FailoverStatus | null = null
  try {
    await waitForCondition(async () => {
      latestStatus = await statusOf(node)
      return latestStatus.peers.some(peer => peer.nodeId === peerId && peer.connected)
    }, timeoutMs)
  } catch (err: unknown) {
    const statusText = JSON.stringify(latestStatus, null, 2)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(`${node.config.nodeId} did not connect to ${peerId}: ${message}\nstatus=${statusText}`)
  }
}

async function executeMajority(node: FailoverNodeProcess, id: number, note: string): Promise<void> {
  await node.execute(
    'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
    [id, node.config.nodeId, id, note],
    { writeConcern: { level: 'majority', timeoutMs: 15_000 } },
  )
}

async function executePublicHttp(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sql,
      writeConcern: { level: 'majority', timeoutMs: 15_000 },
    }),
  })
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`Public HTTP execute on ${nodeId} returned ${response.status}: ${body}`)
  }
}

async function expectPublicHttpRejectsWith(
  environment: GateEnvironment,
  nodeId: string,
  sql: string,
  codes: string[],
): Promise<void> {
  const response = await fetch(`${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sql,
      writeConcern: { level: 'majority', timeoutMs: 15_000 },
    }),
  })
  const data = (await response.json()) as { error?: { code?: string } }
  expect(response.ok).toBe(false)
  expect(codes).toContain(data.error?.code)
}

async function executePublicClient(environment: GateEnvironment, starterNodeId: string, sql: string): Promise<void> {
  const client = new SirannonClient({
    endpoints: [serverBaseUrlFor(environment, starterNodeId)],
    discovery: 'coordinator',
    transport: 'http',
  })
  try {
    const db = client.database(GROUP_ID)
    await db.execute(sql)
  } finally {
    client.close()
  }
}

async function executePublicWebSocket(environment: GateEnvironment, nodeId: string, sql: string): Promise<void> {
  const wsUrl = `ws://127.0.0.1:${httpPortFor(environment, nodeId)}/db/${GROUP_ID}`
  const ws = new WebSocket(wsUrl)
  try {
    await waitForWebSocketOpen(ws)
    const result = await sendWebSocketExecute(ws, sql)
    expect(result.changes).toBe(1)
  } finally {
    ws.close()
  }
}

function waitForWebSocketOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for public WebSocket open')), 10_000)
    ws.addEventListener('open', () => {
      clearTimeout(timeout)
      resolve()
    })
    ws.addEventListener('error', () => {
      clearTimeout(timeout)
      reject(new Error('Public WebSocket failed to open'))
    })
  })
}

function sendWebSocketExecute(ws: WebSocket, sql: string): Promise<{ changes: number }> {
  return new Promise((resolve, reject) => {
    const requestId = `public-ws-${Date.now()}`
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for public WebSocket execute')), 20_000)
    const onMessage = (event: MessageEvent) => {
      const data = JSON.parse(String(event.data)) as {
        type: string
        id: string
        data?: { changes?: number }
        error?: { code?: string; message?: string }
      }
      if (data.id !== requestId) return
      clearTimeout(timeout)
      ws.removeEventListener('message', onMessage)
      if (data.type === 'error') {
        reject(new Error(`Public WebSocket execute failed with ${data.error?.code}: ${data.error?.message}`))
        return
      }
      resolve({ changes: Number(data.data?.changes ?? 0) })
    }
    ws.addEventListener('message', onMessage)
    ws.send(
      JSON.stringify({
        id: requestId,
        type: 'execute',
        sql,
      }),
    )
  })
}

async function waitForRows(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  expectedRows: number,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT COUNT(*) AS count FROM failover_items')
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows[0] ?? null
        if (Number(rows[0]?.count ?? 0) < expectedRows) {
          return false
        }
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for ${expectedRows} failover_items rows on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

async function waitForItemNote(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  itemId: number,
  note: string,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT note FROM failover_items WHERE id = ?', [itemId])
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows
        if (rows[0]?.note !== note) return false
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for failover_items id ${itemId} note '${note}' on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

async function assertItemVisible(node: FailoverNodeProcess, id: number): Promise<void> {
  const rows = await queryRows(node, 'SELECT id FROM failover_items WHERE id = ?', [id])
  expect(rows).toHaveLength(1)
}

async function waitForEvent(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  eventId: number,
  timeoutMs: number,
): Promise<void> {
  const latest: Record<string, unknown> = {}
  try {
    await waitForCondition(async () => {
      for (const nodeId of nodeIds) {
        const node = requireNode(environment, nodeId)
        const rows = await queryRowsWhenReady(node, 'SELECT id FROM failover_events WHERE id = ?', [eventId])
        if (rows === null) {
          latest[nodeId] = await statusOf(node)
          return false
        }
        latest[nodeId] = rows
        if (rows.length === 0) return false
      }
      return true
    }, timeoutMs)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for failover_events id ${eventId} on ${nodeIds.join(
        ',',
      )}: ${message}\nlatest=${JSON.stringify(latest, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

async function runSeededSoak(
  environment: GateEnvironment,
  primaryNodeId: string,
  expectedNodeIds: readonly string[],
): Promise<void> {
  const primary = requireNode(environment, primaryNodeId)
  const latencyNodeId = expectedNodeIds.find(nodeId => nodeId !== primaryNodeId) ?? primaryNodeId
  const proxyName = grpcProxyName(latencyNodeId, primaryNodeId)
  await environment.toxiproxy.addLatency(proxyName, 'seeded-soak-latency', 40)
  try {
    for (let index = 0; index < 12; index++) {
      const id = 20 + index
      await executeMajority(primary, id, `seeded-soak-${index}`)
      if (index % 3 === 0) {
        await primary.execute('UPDATE failover_items SET value = value + 1 WHERE id = ?', [id], {
          writeConcern: { level: 'majority', timeoutMs: 15_000 },
        })
      }
    }
  } finally {
    await environment.toxiproxy.clearToxic(proxyName, 'seeded-soak-latency').catch(() => undefined)
  }
  await waitForRows(environment, expectedNodeIds, 13, 30_000)
}

async function expectResourceUseBelow(environment: GateEnvironment, maxStorageBytes: number): Promise<void> {
  for (const node of environment.nodes.values()) {
    const storageBytes =
      fileSize(node.config.dbPath) + fileSize(`${node.config.dbPath}-wal`) + fileSize(`${node.config.dbPath}-shm`)
    expect(storageBytes).toBeLessThanOrEqual(maxStorageBytes)
  }
  expect(process.memoryUsage().heapUsed).toBeLessThanOrEqual(512 * 1024 * 1024)
}

async function expectExactlyOneWritablePrimary(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: string,
): Promise<void> {
  let latestStatuses: FailoverStatus[] = []
  try {
    await waitForCondition(async () => {
      latestStatuses = await Promise.all([...environment.nodes.values()].map(statusOf))
      const authoritative = latestStatuses.filter(status => status.coordinator?.authority)
      return (
        authoritative.length === 1 &&
        authoritative[0]?.nodeId === expectedPrimary &&
        authoritative[0]?.coordinator?.primaryTerm === expectedTerm
      )
    }, 20_000)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Did not observe exactly one authoritative primary ${expectedPrimary} term ${expectedTerm}: ${message}\nstatuses=${JSON.stringify(
        latestStatuses,
        jsonReplacer,
        2,
      )}`,
    )
  }

  const probeResults: Array<{ nodeId: string; ok: boolean; code?: string; message?: string }> = []
  const nodes = [...environment.nodes.entries()].sort(([left], [right]) => left.localeCompare(right))
  for (const [nodeId, node] of nodes) {
    try {
      await node.localWriteProbe(nextWriteProbeId(), `primary-probe-term-${expectedTerm}-${nodeId}`)
      probeResults.push({ nodeId, ok: true })
    } catch (err: unknown) {
      const error = err as Error & { code?: string }
      probeResults.push({ nodeId, ok: false, code: error.code, message: error.message })
    }
  }

  const writable = probeResults.filter(result => result.ok)
  expect(writable, `local write probe results=${JSON.stringify(probeResults, null, 2)}`).toEqual([
    { nodeId: expectedPrimary, ok: true },
  ])
}

async function assertHealth(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: string,
  expectedReadAvailability: 'available' | 'unavailable',
  expectedWriteAvailability: 'available' | 'unavailable',
): Promise<void> {
  const primaryStatus = await statusOf(requireNode(environment, expectedPrimary))
  expect(primaryStatus.coordinator?.currentPrimary?.nodeId).toBe(expectedPrimary)
  expect(primaryStatus.coordinator?.primaryTerm).toBe(expectedTerm)
  const ready = await readinessOf(environment, expectedPrimary)
  expect(ready.replication?.currentPrimary).toBe(expectedPrimary)
  expect(ready.replication?.primaryTerm).toBe(expectedTerm)
  expect(ready.replication?.readAvailability).toBe(expectedReadAvailability)
  expect(ready.replication?.writeAvailability).toBe(expectedWriteAvailability)
  expect(ready.replication?.syncState).toBe('ready')
  expect(ready.replication?.coordinator?.authority).toBe(true)
  expect(primaryStatus.coordinator?.repairingNodeIds ?? []).toEqual(expect.any(Array))
  expect(primaryStatus.coordinator?.faultedNodeIds ?? []).toEqual(expect.any(Array))
}

async function waitForInSyncSet(
  environment: GateEnvironment,
  expectedNodeIds: readonly string[],
  timeoutMs: number,
): Promise<void> {
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return sameMembers((latestState as { inSyncNodeIds?: string[] } | null)?.inSyncNodeIds ?? [], expectedNodeIds)
    }, timeoutMs)
  } catch (err: unknown) {
    const statuses: Record<string, unknown> = {}
    for (const [nodeId, node] of environment.nodes) {
      statuses[nodeId] = await statusOf(node).catch(statusErr => ({
        error: statusErr instanceof Error ? statusErr.message : String(statusErr),
        logs: node.logs(),
      }))
    }
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for in-sync set ${expectedNodeIds.join(',')}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nstatuses=${JSON.stringify(statuses, jsonReplacer, 2)}`,
    )
  }
}

async function waitForCurrentPrimary(
  environment: GateEnvironment,
  expectedNodeId: string,
  timeoutMs: number,
): Promise<void> {
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return (latestState as { currentPrimary?: { nodeId?: string } } | null)?.currentPrimary?.nodeId === expectedNodeId
    }, timeoutMs)
  } catch (err: unknown) {
    const liveSessions: Record<string, unknown> = {}
    for (const nodeId of NODE_IDS) {
      liveSessions[nodeId] = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId).catch(sessionErr => ({
        error: sessionErr instanceof Error ? sessionErr.message : String(sessionErr),
      }))
    }
    const nodeStatuses: Record<string, unknown> = {}
    for (const [nodeId, node] of environment.nodes) {
      nodeStatuses[nodeId] = await statusOf(node).catch(statusErr => ({
        error: statusErr instanceof Error ? statusErr.message : String(statusErr),
        logs: node.logs(),
      }))
    }
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for primary ${expectedNodeId}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nliveSessions=${JSON.stringify(liveSessions, jsonReplacer, 2)}\nnodeStatuses=${JSON.stringify(
        nodeStatuses,
        jsonReplacer,
        2,
      )}`,
    )
  }
}

async function waitForAnyCurrentPrimary(
  environment: GateEnvironment,
  expectedNodeIds: readonly string[],
  timeoutMs: number,
): Promise<string> {
  let currentPrimary: string | null = null
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      currentPrimary = (latestState as { currentPrimary?: { nodeId?: string } } | null)?.currentPrimary?.nodeId ?? null
      return currentPrimary !== null && expectedNodeIds.includes(currentPrimary)
    }, timeoutMs)
  } catch (err: unknown) {
    const liveSessions: Record<string, unknown> = {}
    for (const nodeId of NODE_IDS) {
      liveSessions[nodeId] = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId).catch(sessionErr => ({
        error: sessionErr instanceof Error ? sessionErr.message : String(sessionErr),
      }))
    }
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for any primary in ${expectedNodeIds.join(',')}: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\nliveSessions=${JSON.stringify(liveSessions, jsonReplacer, 2)}\ndiagnostics=${JSON.stringify(
        diagnostics,
        jsonReplacer,
        2,
      )}`,
    )
  }
  if (currentPrimary === null) {
    throw new Error(`No current primary matched ${expectedNodeIds.join(',')}`)
  }
  return currentPrimary
}

async function healNodeSirannonLinks(environment: GateEnvironment, nodeId: string): Promise<void> {
  await setEtcdLink(environment, nodeId, true)
  for (const otherNodeId of NODE_IDS) {
    if (otherNodeId === nodeId) continue
    await environment.toxiproxy.setEnabled(grpcProxyName(nodeId, otherNodeId), true)
    await environment.toxiproxy.setEnabled(grpcProxyName(otherNodeId, nodeId), true)
  }
}

async function setEtcdLink(environment: GateEnvironment, nodeId: string, enabled: boolean): Promise<void> {
  for (let index = 0; index < 3; index++) {
    await environment.toxiproxy.setEnabled(etcdProxyName(nodeId, index), enabled)
  }
}

async function waitForRecentErrorCountAbove(
  node: FailoverNodeProcess,
  code: string,
  previousCount: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => recentErrorCount(node, code).then(count => count > previousCount), timeoutMs)
}

async function recentErrorCount(node: FailoverNodeProcess, code: string): Promise<number> {
  const status = await statusOf(node)
  return (status.recentErrors ?? []).filter(event => event.error?.code === code).length
}

async function expectStaleBatchRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiver: FailoverNodeProcess,
  receiverNodeId: string,
  senderNodeId: string,
  rowId: number,
): Promise<void> {
  const previousStaleErrors = await recentErrorCount(receiver, 'STALE_PRIMARY')
  await sender.sendRawBatch(receiverNodeId, staleBatch(senderNodeId, 1n, rowId))
  await waitForRecentErrorCountAbove(receiver, 'STALE_PRIMARY', previousStaleErrors, 10_000)
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

async function expectStaleForwardRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  rowId: number,
): Promise<void> {
  await expectRejectsWith(sender.sendRawForward(receiverNodeId, staleForwardedTransaction(rowId, 1n)), [
    'TRANSPORT_ERROR',
  ])
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

async function expectStaleSyncRequestRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  senderNodeId: string,
  rowId: number,
): Promise<void> {
  await sender.requestRawSync(receiverNodeId, staleSyncRequest(senderNodeId, 1n))
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

async function expectStaleSyncBatchRejectedWithoutMutation(
  environment: GateEnvironment,
  sender: FailoverNodeProcess,
  receiverNodeId: string,
  rowId: number,
): Promise<void> {
  await sender.sendRawSyncBatch(receiverNodeId, staleSyncBatch(1n, rowId))
  await assertItemAbsent(environment, [receiverNodeId], rowId, 10_000)
}

async function waitForNoSafePrimary(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const node = requireNode(environment, nodeId)
      if ((await recentErrorCount(node, 'NO_SAFE_PRIMARY')) > 0) {
        return true
      }
    }
    return false
  }, timeoutMs)
}

async function assertItemAbsent(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  itemId: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const node = requireNode(environment, nodeId)
      const rows = await queryRowsWhenReady(node, 'SELECT id FROM failover_items WHERE id = ?', [itemId])
      if (rows === null || rows.length > 0) return false
    }
    return true
  }, timeoutMs)
}

async function expectRejectsWith(promise: Promise<unknown>, codes: string[]): Promise<void> {
  try {
    await promise
  } catch (err: unknown) {
    const code = (err as { code?: string }).code
    expect(codes).toContain(code)
    return
  }
  throw new Error(`Expected operation to reject with one of: ${codes.join(', ')}`)
}

async function queryRows(
  node: FailoverNodeProcess,
  sql: string,
  params?: unknown[],
): Promise<Array<Record<string, unknown>>> {
  const value = await node.query(sql, params, { readConcern: { level: 'local' } })
  if (!Array.isArray(value)) {
    throw new Error('Query did not return an array')
  }
  return value as Array<Record<string, unknown>>
}

async function queryRowsWhenReady(
  node: FailoverNodeProcess,
  sql: string,
  params?: unknown[],
): Promise<Array<Record<string, unknown>> | null> {
  const status = await statusOf(node)
  if (status.syncState?.phase !== 'ready') {
    return null
  }
  try {
    return await queryRows(node, sql, params)
  } catch (err: unknown) {
    if (isSyncPhaseReadError(err)) return null
    throw err
  }
}

async function statusOf(node: FailoverNodeProcess): Promise<FailoverStatus> {
  return (await node.status()) as unknown as FailoverStatus
}

async function collectNodeDiagnostics(environment: GateEnvironment): Promise<Record<string, unknown>> {
  const diagnostics: Record<string, unknown> = {}
  for (const [nodeId, node] of environment.nodes) {
    diagnostics[nodeId] = await statusOf(node).catch(err => ({
      error: err instanceof Error ? err.message : String(err),
      logs: node.logs(),
    }))
  }
  return diagnostics
}

async function readinessOf(environment: GateEnvironment, nodeId: string): Promise<ReadinessResponse> {
  const response = await fetch(`http://127.0.0.1:${httpPortFor(environment, nodeId)}/health/ready`)
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Readiness for ${nodeId} returned ${response.status}: ${text}`)
  }
  return JSON.parse(text) as ReadinessResponse
}

function isSyncPhaseReadError(err: unknown): boolean {
  const error = err as Error & { code?: string }
  return error.code === 'SYNC_ERROR' && error.message.includes('cannot serve reads')
}

function requireNode(environment: GateEnvironment, nodeId: string): FailoverNodeProcess {
  const node = environment.nodes.get(nodeId)
  if (!node) throw new Error(`Node ${nodeId} is not running`)
  return node
}

function endpointFor(environment: GateEnvironment, fromNodeId: string, toNodeId: string): string {
  const proxyPort = environment.grpcProxyPorts.get(fromNodeId)?.get(toNodeId)
  if (proxyPort === undefined) {
    throw new Error(`Missing gRPC proxy ${fromNodeId} -> ${toNodeId}`)
  }
  return `127.0.0.1:${proxyPort}`
}

function httpPortFor(environment: GateEnvironment, nodeId: string): number {
  const port = environment.httpPorts.get(nodeId)
  if (port === undefined) {
    throw new Error(`Missing HTTP port for ${nodeId}`)
  }
  return port
}

function serverBaseUrlFor(environment: GateEnvironment, nodeId: string): string {
  return `http://127.0.0.1:${httpPortFor(environment, nodeId)}`
}

function httpEndpointFor(environment: GateEnvironment, nodeId: string): string {
  return `${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}`
}

function etcdEndpointsFor(environment: GateEnvironment, nodeId: string): string[] {
  const ports = environment.etcdProxyPorts.get(nodeId)
  if (!ports) throw new Error(`Missing etcd endpoints for ${nodeId}`)
  return ports.map(port => `http://127.0.0.1:${port}`)
}

function etcdProxyName(nodeId: string, memberIndex: number): string {
  return `${nodeId}-to-etcd-${memberIndex + 1}`
}

function grpcProxyName(fromNodeId: string, toNodeId: string): string {
  return `${fromNodeId}-to-${toNodeId}`
}

function staleBatch(sourceNodeId: string, primaryTerm: bigint, rowId: number): ReplicationBatch {
  const change = staleInsertChange(sourceNodeId, rowId, 'stale-batch')
  const changes = [change]
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-1-1`,
    fromSeq: 1n,
    toSeq: 1n,
    hlcRange: {
      min: `${Date.now().toString(16).padStart(12, '0')}-0000-${sourceNodeId}`,
      max: `${Date.now().toString(16).padStart(12, '0')}-0000-${sourceNodeId}`,
    },
    changes,
    checksum: createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex'),
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleForwardedTransaction(rowId: number, primaryTerm: bigint): ForwardedTransaction {
  return {
    requestId: `stale-forward-${rowId}`,
    statements: [
      {
        sql: 'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        params: [rowId, 'forward-stale', rowId, 'stale-forward'],
      },
    ],
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleSyncRequest(joinerNodeId: string, primaryTerm: bigint): SyncRequest {
  return {
    requestId: `${joinerNodeId}-stale-sync`,
    joinerNodeId,
    completedTables: [],
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleSyncBatch(primaryTerm: bigint, rowId: number): SyncBatch {
  const rows = [
    {
      id: rowId,
      owner: 'sync-stale',
      value: rowId,
      note: 'stale-sync-batch',
    },
  ]
  return {
    requestId: 'stale-sync-batch',
    table: 'failover_items',
    batchIndex: 0,
    rows,
    checksum: createHash('sha256').update(canonicaliseForChecksum(rows)).digest('hex'),
    isLastBatchForTable: true,
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function staleInsertChange(sourceNodeId: string, rowId: number, note: string): ReplicationChange {
  const hlc = `${Date.now().toString(16).padStart(12, '0')}-0000-${sourceNodeId}`
  return {
    table: 'failover_items',
    operation: 'insert',
    rowId: String(rowId),
    primaryKey: { id: rowId },
    hlc,
    txId: `${sourceNodeId}-stale-${rowId}`,
    nodeId: sourceNodeId,
    newData: {
      id: rowId,
      owner: sourceNodeId,
      value: rowId,
      note,
    },
    oldData: null,
  }
}

function sameMembers(actual: readonly string[], expected: readonly string[]): boolean {
  if (actual.length !== expected.length) return false
  const actualSet = new Set(actual)
  return expected.every(value => actualSet.has(value))
}

function zip<T, U>(left: readonly T[], right: readonly U[]): Array<[T, U]> {
  return left.map((value, index) => {
    const paired = right[index]
    if (paired === undefined) {
      throw new Error('Cannot zip arrays of different lengths')
    }
    return [value, paired]
  })
}

function fileSize(path: string): number {
  return existsSync(path) ? statSync(path).size : 0
}

function nextWriteProbeId(): number {
  writeProbeId += 1
  return writeProbeId
}

function jsonReplacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value
}

async function waitForCondition(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastError: Error | null = null

  while (Date.now() < deadline) {
    try {
      if (await predicate()) return
    } catch (err: unknown) {
      lastError = err instanceof Error ? err : new Error(String(err))
    }
    await sleep(50)
  }

  if (lastError) throw lastError
  throw new Error(`Condition was not met within ${timeoutMs}ms`)
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms) as ReturnType<typeof setTimeout> & {
      unref?: () => void
    }
    timer.unref?.()
  })
}
