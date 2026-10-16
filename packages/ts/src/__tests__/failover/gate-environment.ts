import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createEtcdCoordinator, type EtcdClusterCoordinator } from '../../replication/coordinator/etcd.js'
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
  unpauseContainer,
} from './docker.js'
import { EtcdOracle } from './etcd-oracle.js'
import { zip } from './gate-support.js'
import { type EtcdAccess, type FailoverNodeConfig, FailoverNodeProcess, type FailoverNodeRole } from './node-process.js'
import { ToxiproxyClient } from './toxiproxy.js'

export const NODE_A = 'failover-node-a'
export const NODE_B = 'failover-node-b'
export const NODE_C = 'failover-node-c'
export const GROUP_ID = 'orders'
export const CLUSTER_ID = 'cluster-a'
export const NODE_IDS = [NODE_A, NODE_B, NODE_C] as const
export const COMPATIBILITY = {
  packageVersion: '1.0.0',
  specVersion: '1.0.0',
  protocolVersion: '1.0.0',
}

export interface GateEnvironment {
  runPrefix: string
  certs: MtlsCerts
  etcd: EtcdClusterHandle
  toxiproxyContainer: ToxiproxyContainerHandle
  toxiproxy: ToxiproxyClient
  grpcPorts: Map<string, number>
  httpPorts: Map<string, number>
  etcdProxyPorts: Map<string, number[]>
  grpcProxyPorts: Map<string, Map<string, number>>
  scenario: number
  tempDir: string
  keyPrefix: string
  coordinator: EtcdClusterCoordinator
  etcdOracle: EtcdOracle
  nodes: Map<string, FailoverNodeProcess>
}

export interface NodeOptions {
  role?: FailoverNodeRole
  dialTargets?: readonly string[]
  seedSchema?: boolean
  createsGroup?: boolean
  voters?: readonly string[]
  sessionTtlMs?: number
  controllerLeaseTtlMs?: number
  controllerTickIntervalMs?: number
  controllerEnabled?: boolean
  writeForwarding?: boolean
  compatibility?: FailoverNodeConfig['compatibility']
  stableNodeId?: boolean
  etcdAccess?: EtcdAccess
  backups?: boolean
  clockOffsetMs?: number
  etcd3Unresolvable?: boolean
  awaitReady?: boolean
  initialSync?: boolean
}

export async function startEnvironment(): Promise<GateEnvironment> {
  await ensureFailoverImages()
  const runPrefix = failoverRunPrefix()
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
      await toxiproxy.createProxy({ name: etcdProxyName(nodeId, index), listenPort, upstreamPort })
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
      await toxiproxy.createProxy({ name: grpcProxyName(fromNodeId, toNodeId), listenPort, upstreamPort })
    }
  }

  const keyPrefix = `${runPrefix}/scenario-0`
  return {
    runPrefix,
    certs,
    etcd,
    toxiproxyContainer,
    toxiproxy,
    grpcPorts,
    httpPorts,
    etcdProxyPorts,
    grpcProxyPorts,
    scenario: 0,
    tempDir: mkdtempSync(join(tmpdir(), `${runPrefix}-`)),
    keyPrefix,
    coordinator: operatorCoordinator(etcd, keyPrefix),
    etcdOracle: new EtcdOracle(etcd.endpoints, keyPrefix, CLUSTER_ID, GROUP_ID),
    nodes: new Map(),
  }
}

export async function beginScenario(environment: GateEnvironment): Promise<void> {
  await endScenario(environment)
  environment.scenario += 1
  environment.keyPrefix = `${environment.runPrefix}/scenario-${environment.scenario}`
  environment.tempDir = mkdtempSync(join(tmpdir(), `${environment.runPrefix}-${environment.scenario}-`))
  environment.coordinator = operatorCoordinator(environment.etcd, environment.keyPrefix)
  environment.etcdOracle = new EtcdOracle(environment.etcd.endpoints, environment.keyPrefix, CLUSTER_ID, GROUP_ID)
}

export async function endScenario(environment: GateEnvironment): Promise<void> {
  await Promise.all([...environment.nodes.values()].map(node => node.killAndWait()))
  environment.nodes.clear()
  await environment.coordinator.close().catch(() => undefined)
  for (const containerName of environment.etcd.containerNames) {
    await unpauseContainer(containerName)
  }
  await environment.toxiproxy.reset()
  rmSync(environment.tempDir, { recursive: true, force: true })
}

export async function cleanupEnvironment(environment: GateEnvironment): Promise<void> {
  await endScenario(environment).catch(() => undefined)
  environment.certs.cleanup()
  await cleanupDockerResources({
    containers: [...environment.etcd.containerNames, environment.toxiproxyContainer.containerName],
    networks: [environment.etcd.networkName],
  })
}

export async function startNode(
  environment: GateEnvironment,
  nodeId: string,
  options: NodeOptions = {},
): Promise<FailoverNodeProcess> {
  const cert = environment.certs.certForNode(nodeId)
  const grpcPort = environment.grpcPorts.get(nodeId)
  if (grpcPort === undefined) throw new Error(`Missing gRPC port for ${nodeId}`)
  const dialTargets = options.dialTargets ?? NODE_IDS.filter(target => target !== nodeId)

  const config: FailoverNodeConfig = {
    nodeId,
    stableNodeId: options.stableNodeId ?? true,
    dbPath: dbPathFor(environment, nodeId),
    grpcPort,
    httpPort: httpPortFor(environment, nodeId),
    certPath: cert.certPath,
    keyPath: cert.keyPath,
    caCertPath: environment.certs.caCertPath,
    initialRole: options.role ?? 'replica',
    endpoints: dialTargets.map(target => endpointFor(environment, nodeId, target)),
    httpEndpoints: Object.fromEntries(NODE_IDS.map(target => [target, httpEndpointFor(environment, target)])),
    etcdHosts: etcdEndpointsFor(environment, nodeId),
    etcdAccess: options.etcdAccess ?? 'insecure-test',
    keyPrefix: environment.keyPrefix,
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    votingDataBearingNodeIds: options.createsGroup ? [...(options.voters ?? NODE_IDS)] : undefined,
    seedSchema: options.seedSchema ?? false,
    sessionTtlMs: options.sessionTtlMs ?? 1_500,
    controllerLeaseTtlMs: options.controllerLeaseTtlMs ?? 1_500,
    controllerTickIntervalMs: options.controllerTickIntervalMs ?? 200,
    controllerEnabled: options.controllerEnabled ?? true,
    writeForwarding: options.writeForwarding ?? true,
    initialSync: options.initialSync ?? true,
    backupDirectory: options.backups ? backupDirectoryFor(environment) : undefined,
    compatibility: options.compatibility ?? COMPATIBILITY,
  }

  const node = new FailoverNodeProcess(config, {
    clockOffsetMs: options.clockOffsetMs,
    etcd3Unresolvable: options.etcd3Unresolvable,
  })
  environment.nodes.set(nodeId, node)
  if (options.awaitReady ?? true) {
    await node.ready()
  }
  return node
}

export async function killNode(environment: GateEnvironment, nodeId: string): Promise<void> {
  const node = environment.nodes.get(nodeId)
  if (!node) return
  await node.killAndWait()
  environment.nodes.delete(nodeId)
}

export function operatorCoordinator(etcd: EtcdClusterHandle, keyPrefix: string): EtcdClusterCoordinator {
  return createEtcdCoordinator({
    hosts: etcd.endpoints,
    keyPrefix,
    allowInsecure: true,
    dialTimeoutMs: 1_000,
    defaultCallTimeoutMs: 2_000,
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

export async function healNodeSirannonLinks(environment: GateEnvironment, nodeId: string): Promise<void> {
  await setEtcdLink(environment, nodeId, true)
  await setReplicationLinks(environment, nodeId, true)
}

export async function setReplicationLinks(
  environment: GateEnvironment,
  nodeId: string,
  enabled: boolean,
  peers: readonly string[] = NODE_IDS.filter(otherNodeId => otherNodeId !== nodeId),
): Promise<void> {
  for (const otherNodeId of peers) {
    if (otherNodeId === nodeId) continue
    await environment.toxiproxy.setEnabled(grpcProxyName(nodeId, otherNodeId), enabled)
    await environment.toxiproxy.setEnabled(grpcProxyName(otherNodeId, nodeId), enabled)
  }
}

export async function setEtcdLink(environment: GateEnvironment, nodeId: string, enabled: boolean): Promise<void> {
  for (let index = 0; index < 3; index++) {
    await environment.toxiproxy.setEnabled(etcdProxyName(nodeId, index), enabled)
  }
}

export function requireNode(environment: GateEnvironment, nodeId: string): FailoverNodeProcess {
  const node = environment.nodes.get(nodeId)
  if (!node) throw new Error(`Node ${nodeId} is not running`)
  return node
}

export function dbPathFor(environment: GateEnvironment, nodeId: string): string {
  return join(environment.tempDir, `${nodeId}.db`)
}

export function backupDirectoryFor(environment: GateEnvironment): string {
  return join(environment.tempDir, 'backups')
}

export function endpointFor(environment: GateEnvironment, fromNodeId: string, toNodeId: string): string {
  const proxyPort = environment.grpcProxyPorts.get(fromNodeId)?.get(toNodeId)
  if (proxyPort === undefined) {
    throw new Error(`Missing gRPC proxy ${fromNodeId} -> ${toNodeId}`)
  }
  return `127.0.0.1:${proxyPort}`
}

export function httpPortFor(environment: GateEnvironment, nodeId: string): number {
  const port = environment.httpPorts.get(nodeId)
  if (port === undefined) {
    throw new Error(`Missing HTTP port for ${nodeId}`)
  }
  return port
}

export function serverBaseUrlFor(environment: GateEnvironment, nodeId: string): string {
  return `http://127.0.0.1:${httpPortFor(environment, nodeId)}`
}

export function httpEndpointFor(environment: GateEnvironment, nodeId: string): string {
  return `${serverBaseUrlFor(environment, nodeId)}/db/${GROUP_ID}`
}

export function etcdEndpointsFor(environment: GateEnvironment, nodeId: string): string[] {
  const ports = environment.etcdProxyPorts.get(nodeId)
  if (!ports) throw new Error(`Missing etcd endpoints for ${nodeId}`)
  return ports.map(port => `http://127.0.0.1:${port}`)
}

export function etcdProxyName(nodeId: string, memberIndex: number): string {
  return `${nodeId}-to-etcd-${memberIndex + 1}`
}

export function grpcProxyName(fromNodeId: string, toNodeId: string): string {
  return `${fromNodeId}-to-${toNodeId}`
}
