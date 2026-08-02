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
} from './docker.js'
import { zip } from './gate-support.js'
import { type FailoverNodeConfig, FailoverNodeProcess } from './node-process.js'
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

export async function startEnvironment(): Promise<GateEnvironment> {
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

export async function startNode(
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

export async function cleanupEnvironment(environment: GateEnvironment): Promise<void> {
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

export async function healNodeSirannonLinks(environment: GateEnvironment, nodeId: string): Promise<void> {
  await setEtcdLink(environment, nodeId, true)
  for (const otherNodeId of NODE_IDS) {
    if (otherNodeId === nodeId) continue
    await environment.toxiproxy.setEnabled(grpcProxyName(nodeId, otherNodeId), true)
    await environment.toxiproxy.setEnabled(grpcProxyName(otherNodeId, nodeId), true)
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
