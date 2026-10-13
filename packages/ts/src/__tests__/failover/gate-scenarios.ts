import { waitForInSyncSet } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
  healNodeSirannonLinks,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  type NodeOptions,
  requireNode,
  startNode,
} from './gate-environment.js'
import { waitForNodeReady, waitForPeerConnected } from './gate-observations.js'
import { waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'

export interface ClusterOptions {
  everyNode?: NodeOptions
  perNode?: Partial<Record<string, NodeOptions>>
  voters?: readonly string[]
  members?: readonly string[]
  awaitInSync?: boolean
}

export async function startCluster(environment: GateEnvironment, options: ClusterOptions = {}): Promise<void> {
  const members = options.members ?? NODE_IDS
  const voters = options.voters ?? NODE_IDS
  const nodeOptions = (nodeId: string): NodeOptions => ({ ...options.everyNode, ...options.perNode?.[nodeId] })
  const [first, ...rest] = members
  if (first === undefined) throw new Error('A cluster needs at least one member')

  const primary = await startNode(environment, first, {
    role: 'primary',
    seedSchema: true,
    createsGroup: true,
    voters,
    dialTargets: [],
    ...nodeOptions(first),
  })
  await waitForNodeReady(primary)

  for (const nodeId of rest) {
    const node = await startNode(environment, nodeId, {
      role: 'replica',
      dialTargets: members.filter(target => target !== nodeId),
      ...nodeOptions(nodeId),
    })
    await waitForNodeReady(node, 30_000)
  }

  for (const nodeId of rest.slice(0, -1)) {
    const node = requireNode(environment, nodeId)
    await node.reconnectTransport()
    for (const peerId of members) {
      if (peerId !== nodeId) await waitForPeerConnected(node, peerId, 15_000)
    }
  }

  if (options.awaitInSync ?? true) {
    await waitForInSyncSet(environment, members, 30_000)
  }
}

export async function startThreeNodeCluster(environment: GateEnvironment, everyNode: NodeOptions = {}): Promise<void> {
  await startCluster(environment, { everyNode, members: [NODE_A, NODE_B, NODE_C] })
}

export async function restartNode(
  environment: GateEnvironment,
  nodeId: string,
  options: NodeOptions = {},
): Promise<FailoverNodeProcess> {
  await healNodeSirannonLinks(environment, nodeId)
  await waitForCondition(async () => (await environment.etcdOracle.nodeSession(nodeId)) === null, 15_000)
  const node = await startNode(environment, nodeId, { role: 'replica', ...options })
  await waitForNodeReady(node, 30_000)
  return node
}

export async function rebuildThroughRepair(
  environment: GateEnvironment,
  nodeId: string,
  currentPrimaryNodeId: string,
  options: NodeOptions = {},
): Promise<FailoverNodeProcess> {
  await environment.coordinator.updateNodeMaintenance({
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    nodeId,
    draining: false,
    repairing: true,
    faulted: false,
  })
  await killNode(environment, nodeId)
  const restarted = await restartNode(environment, nodeId, options)
  await waitForPeerConnected(restarted, currentPrimaryNodeId, 15_000)
  return restarted
}
