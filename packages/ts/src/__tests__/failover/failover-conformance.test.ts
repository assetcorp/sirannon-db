import { createHash } from 'node:crypto'
import { existsSync, mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { createEtcdCoordinator, type EtcdClusterCoordinator } from '../../replication/coordinator/etcd.js'
import { canonicaliseForChecksum } from '../../replication/log.js'
import type { ReplicationBatch, SyncBatch, SyncRequest } from '../../replication/types.js'
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

let env: GateEnvironment | null = null

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
    await assertHealth(env, NODE_A, '1', 'available', 'available')

    await setEtcdLink(env, NODE_A, false)
    await expectRejectsWith(
      nodeA.execute("INSERT INTO failover_items (id, owner, value, note) VALUES (99, 'a', 99, 'stale')"),
      ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY'],
    )

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

    await forwardingNode.sendRawBatch(promotedNodeId, staleBatch(forwardingNodeId, 1n)).catch(() => undefined)
    await forwardingNode.requestRawSync(promotedNodeId, staleSyncRequest(forwardingNodeId, 1n)).catch(() => undefined)
    await forwardingNode.sendRawSyncBatch(promotedNodeId, staleSyncBatch(1n)).catch(() => undefined)
    await waitForRecentErrorCode(promotedNode, 'STALE_PRIMARY', 10_000)

    await forwardingNode.execute(
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (10, 1, 'forwarded', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 10, 20_000)

    await healNodeSirannonLinks(env, NODE_A)
    const restartedA = await startNode(env, NODE_A, 'primary', [NODE_B, NODE_C], false, false)
    await waitForNodeReady(restartedA, 30_000)
    await waitForInSyncSet(env, [NODE_A, promotedNodeId], 30_000)
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

    await runSeededSoak(env, NODE_A)
    await expectResourceUseBelow(env, 256 * 1024 * 1024)

    await env.coordinator.updateInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      inSyncNodeIds: [NODE_A],
    })
    restartedA.kill()
    env.nodes.delete(NODE_A)
    await waitForCondition(async () => {
      const state = await env?.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return state?.currentPrimary?.nodeId === NODE_A
    }, 10_000)
    await expectRejectsWith(
      nodeC.execute("INSERT INTO failover_items (id, owner, value, note) VALUES (80, 'c', 80, 'unsafe')"),
      ['TOPOLOGY_ERROR', 'STALE_PRIMARY', 'NO_SAFE_PRIMARY'],
    )

    await env.coordinator.setReplicationGroupState({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      votingDataBearingNodeIds: [...NODE_IDS],
      currentPrimary: { nodeId: NODE_C, endpoint: `127.0.0.1:${env.grpcPorts.get(NODE_C) ?? 0}` },
      primaryTerm: 4n,
      inSyncNodeIds: [NODE_C],
      compatibility: {
        packageVersion: '2.0.0',
        specVersion: '2.0.0',
        protocolVersion: '2.0.0',
      },
    })
    await waitForCurrentPrimary(env, NODE_C, 10_000)
    await expectRejectsWith(
      nodeC.execute(
        "INSERT INTO failover_items (id, owner, value, note) VALUES (90, 'c', 90, 'incompatible')",
        undefined,
        {
          writeConcern: { level: 'local' },
        },
      ),
      ['PROTOCOL_VERSION_MISMATCH'],
    )
  })
})

async function startEnvironment(): Promise<GateEnvironment> {
  await ensureFailoverImages()
  const runPrefix = failoverRunPrefix()
  const tempDir = mkdtempSync(join(tmpdir(), `${runPrefix}-`))
  const certs = await createMtlsCerts(NODE_IDS)
  const etcd = await startEtcdCluster(runPrefix)
  const grpcPorts = new Map<string, number>(zip(NODE_IDS, await allocatePorts(NODE_IDS.length)))
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
    certPath: cert.certPath,
    keyPath: cert.keyPath,
    caCertPath: environment.certs.caCertPath,
    initialRole: role,
    endpoints: endpointTargets.map(target => endpointFor(environment, nodeId, target)),
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

async function waitForRows(
  environment: GateEnvironment,
  nodeIds: readonly string[],
  expectedRows: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const node = requireNode(environment, nodeId)
      const rows = await queryRows(node, 'SELECT COUNT(*) AS count FROM failover_items')
      if (Number(rows[0]?.count ?? 0) < expectedRows) {
        return false
      }
    }
    return true
  }, timeoutMs)
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
  await waitForCondition(async () => {
    for (const nodeId of nodeIds) {
      const rows = await queryRows(requireNode(environment, nodeId), 'SELECT id FROM failover_events WHERE id = ?', [
        eventId,
      ])
      if (rows.length === 0) return false
    }
    return true
  }, timeoutMs)
}

async function runSeededSoak(environment: GateEnvironment, primaryNodeId: string): Promise<void> {
  const primary = requireNode(environment, primaryNodeId)
  const proxyName = grpcProxyName(NODE_C, primaryNodeId)
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
  await waitForRows(
    environment,
    [NODE_A, NODE_B, NODE_C].filter(nodeId => environment.nodes.has(nodeId)),
    13,
    30_000,
  )
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
  await waitForCondition(async () => {
    const statuses = await Promise.all([...environment.nodes.values()].map(statusOf))
    const authoritative = statuses.filter(status => status.coordinator?.authority)
    return (
      authoritative.length === 1 &&
      authoritative[0]?.nodeId === expectedPrimary &&
      authoritative[0]?.coordinator?.primaryTerm === expectedTerm
    )
  }, 20_000)
}

async function assertHealth(
  environment: GateEnvironment,
  expectedPrimary: string,
  expectedTerm: string,
  expectedReadAvailability: 'available' | 'unavailable',
  expectedWriteAvailability: 'available' | 'unavailable',
): Promise<void> {
  const primary = await statusOf(requireNode(environment, expectedPrimary))
  expect(primary.coordinator?.currentPrimary?.nodeId).toBe(expectedPrimary)
  expect(primary.coordinator?.primaryTerm).toBe(expectedTerm)
  expect(readAvailability(primary)).toBe(expectedReadAvailability)
  expect(writeAvailability(primary)).toBe(expectedWriteAvailability)
  expect(primary.coordinator?.repairingNodeIds ?? []).toEqual(expect.any(Array))
  expect(primary.coordinator?.faultedNodeIds ?? []).toEqual(expect.any(Array))
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
  await waitForCondition(async () => {
    const state = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
    currentPrimary = state?.currentPrimary?.nodeId ?? null
    return currentPrimary !== null && expectedNodeIds.includes(currentPrimary)
  }, timeoutMs)
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

async function waitForRecentErrorCode(node: FailoverNodeProcess, code: string, timeoutMs: number): Promise<void> {
  await waitForCondition(async () => {
    const status = await statusOf(node)
    return (status.recentErrors ?? []).some(event => event.error?.code === code)
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

async function statusOf(node: FailoverNodeProcess): Promise<FailoverStatus> {
  return (await node.status()) as unknown as FailoverStatus
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

function staleBatch(sourceNodeId: string, primaryTerm: bigint): ReplicationBatch {
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-1-1`,
    fromSeq: 1n,
    toSeq: 1n,
    hlcRange: {
      min: `${Date.now().toString(16).padStart(12, '0')}-0000-${sourceNodeId}`,
      max: `${Date.now().toString(16).padStart(12, '0')}-0000-${sourceNodeId}`,
    },
    changes: [],
    checksum: createHash('sha256').update(canonicaliseForChecksum([])).digest('hex'),
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

function staleSyncBatch(primaryTerm: bigint): SyncBatch {
  return {
    requestId: 'stale-sync-batch',
    table: 'failover_items',
    batchIndex: 0,
    rows: [],
    checksum: createHash('sha256').update(canonicaliseForChecksum([])).digest('hex'),
    isLastBatchForTable: true,
    groupId: GROUP_ID,
    primaryTerm,
  }
}

function readAvailability(status: FailoverStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return coordinator.inSyncNodeIds.includes(status.nodeId) ? 'available' : 'unavailable'
}

function writeAvailability(status: FailoverStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (!coordinator.authority) return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
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
