import { describe, expect, it } from 'vitest'
import { toClusterReadEndpoints, toClusterStatusInfo, toReplicationStatusInfo } from '../cluster-status.js'
import type { CoordinatorRuntimeStatus, ReplicationStatus } from '../types.js'

const ENDPOINTS = {
  'node-a': 'https://a.example:8080',
  'node-b': 'https://b.example:8080',
  'node-c': 'https://c.example:8080',
}

function coordinatorStatus(overrides: Partial<CoordinatorRuntimeStatus> = {}): CoordinatorRuntimeStatus {
  return {
    clusterId: 'cluster-1',
    groupId: 'orders',
    currentPrimary: { nodeId: 'node-a', endpoint: ENDPOINTS['node-a'] },
    primaryTerm: 7n,
    inSyncNodeIds: ['node-a', 'node-b'],
    drainingNodeIds: [],
    repairingNodeIds: [],
    faultedNodeIds: [],
    votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
    authority: true,
    connected: true,
    controllerState: 'active',
    ...overrides,
  }
}

function engineStatus(overrides: Partial<ReplicationStatus> = {}): ReplicationStatus {
  return {
    nodeId: 'node-a',
    role: 'primary',
    peers: [],
    localSeq: 42n,
    replicating: true,
    health: { state: 'healthy', reason: 'in-sync', canRead: true, canWrite: true },
    coordinator: coordinatorStatus(),
    ...overrides,
  }
}

describe('the nodes a client can read from', () => {
  it('serves majority reads from an in-sync node and local reads from one that fell behind', () => {
    const endpoints = toClusterReadEndpoints(coordinatorStatus(), ENDPOINTS)

    expect(endpoints).toEqual([
      { nodeId: 'node-a', endpoint: ENDPOINTS['node-a'], readConcerns: ['local', 'majority'] },
      { nodeId: 'node-b', endpoint: ENDPOINTS['node-b'], readConcerns: ['local', 'majority'] },
      { nodeId: 'node-c', endpoint: ENDPOINTS['node-c'], readConcerns: ['local'] },
    ])
  })

  it('leaves out a quarantined node, one being taken out of service, and one being rebuilt', () => {
    const endpoints = toClusterReadEndpoints(
      coordinatorStatus({ faultedNodeIds: ['node-b'], drainingNodeIds: ['node-c'] }),
      ENDPOINTS,
    )
    expect(endpoints.map(entry => entry.nodeId)).toEqual(['node-a'])

    const rebuilding = toClusterReadEndpoints(coordinatorStatus({ repairingNodeIds: ['node-c'] }), ENDPOINTS)
    expect(rebuilding.map(entry => entry.nodeId)).toEqual(['node-a', 'node-b'])
  })

  it('leaves out a node that does not count towards majority', () => {
    const endpoints = toClusterReadEndpoints(
      coordinatorStatus({ votingDataBearingNodeIds: ['node-a', 'node-b'] }),
      ENDPOINTS,
    )
    expect(endpoints.map(entry => entry.nodeId)).toEqual(['node-a', 'node-b'])
  })

  it('reports an empty address for a node the caller gave none for', () => {
    const endpoints = toClusterReadEndpoints(coordinatorStatus(), { 'node-a': ENDPOINTS['node-a'] })
    expect(endpoints.map(entry => entry.endpoint)).toEqual([ENDPOINTS['node-a'], '', ''])
  })
})

describe('the group status one node reports', () => {
  it('carries the group, the primary, its term, and the readable nodes', () => {
    const info = toClusterStatusInfo(engineStatus(), { databaseId: 'orders', endpoints: ENDPOINTS })

    expect(info.databaseId).toBe('orders')
    expect(info.replicationGroupId).toBe('orders')
    expect(info.role).toBe('primary')
    expect(info.currentPrimary).toEqual({ nodeId: 'node-a', endpoint: ENDPOINTS['node-a'] })
    expect(info.primaryTerm).toBe(7n)
    expect(info.readEndpoints).toHaveLength(3)
    expect(info.health).toBe('healthy')
    expect(info.healthReason).toBe('in-sync')
  })

  it('reports no group and no readable nodes for a node running without a coordinator', () => {
    const info = toClusterStatusInfo(engineStatus({ coordinator: undefined }), {
      databaseId: 'orders',
      endpoints: ENDPOINTS,
    })

    expect(info.replicationGroupId).toBeUndefined()
    expect(info.currentPrimary).toBeNull()
    expect(info.readEndpoints).toBeUndefined()
    expect(info.health).toBe('healthy')
  })

  it('reports no primary when the group names none', () => {
    const info = toClusterStatusInfo(
      engineStatus({
        health: { state: 'unavailable', reason: 'no-group-state', canRead: false, canWrite: false },
        coordinator: coordinatorStatus({ currentPrimary: null }),
      }),
      { databaseId: 'orders', endpoints: ENDPOINTS },
    )

    expect(info.currentPrimary).toBeNull()
    expect(info.health).toBe('unavailable')
  })
})

describe('the replication figures one node reports', () => {
  it('separates the primary from its in-sync replicas and names the ones lagging', () => {
    const info = toReplicationStatusInfo(engineStatus())

    expect(info.inSyncReplicas).toEqual(['node-b'])
    expect(info.laggingReplicas).toEqual(['node-c'])
    expect(info.currentPrimary).toBe('node-a')
    expect(info.primaryTerm).toBe(7n)
    expect(info.localSeq).toBe(42n)
    expect(info.coordinator).toEqual({ connected: true, authority: true })
    expect(info.controller).toEqual({ state: 'active' })
  })

  it('omits every coordinator figure for a node running without one', () => {
    const info = toReplicationStatusInfo(engineStatus({ coordinator: undefined }))

    expect(info.replicationGroupId).toBeUndefined()
    expect(info.coordinator).toBeUndefined()
    expect(info.controller).toBeUndefined()
    expect(info.inSyncReplicas).toBeUndefined()
    expect(info.laggingReplicas).toBeUndefined()
    expect(info.role).toBe('primary')
  })

  it('reports a node that lost its coordinator as disconnected', () => {
    const info = toReplicationStatusInfo(
      engineStatus({ coordinator: coordinatorStatus({ connected: false, authority: false }) }),
    )

    expect(info.coordinator).toEqual({ connected: false, authority: false })
  })
})
