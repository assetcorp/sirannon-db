import { describe, expect, it } from 'vitest'
import { getMajorityWriteAvailability } from '../../examples/distributed-entitlements/src/lib/cluster-readiness.js'
import type { ClusterNode } from '../../examples/distributed-entitlements/src/lib/schemas.js'

function node(nodeId: string, health: ClusterNode['health'], options: Partial<ClusterNode> = {}): ClusterNode {
  return {
    nodeId,
    endpoint: `http://127.0.0.1:${nodeId === 'node-a' ? '7301' : nodeId === 'node-b' ? '7302' : '7303'}`,
    reachable: true,
    role: nodeId === 'node-a' ? 'primary' : 'replica',
    health,
    currentPrimary: 'node-a',
    primaryTerm: '1',
    readEndpoints: 2,
    error: null,
    ...options,
  }
}

describe('distributed entitlements majority readiness', () => {
  it('blocks writes while both replicas are syncing', () => {
    const availability = getMajorityWriteAvailability([
      node('node-a', 'healthy'),
      node('node-b', 'syncing'),
      node('node-c', 'syncing'),
    ])

    expect(availability).toMatchObject({
      available: false,
      healthyVoters: 1,
      requiredVoters: 2,
      reason: 'Waiting for majority: 1/2 healthy voters',
    })
  })

  it('allows writes when the primary and one replica are healthy', () => {
    const availability = getMajorityWriteAvailability([
      node('node-a', 'healthy'),
      node('node-b', 'healthy'),
      node('node-c', 'syncing'),
    ])

    expect(availability.available).toBe(true)
    expect(availability.healthyVoters).toBe(2)
  })

  it('blocks writes when reachable nodes disagree on the primary term', () => {
    const availability = getMajorityWriteAvailability([
      node('node-a', 'healthy'),
      node('node-b', 'healthy', { primaryTerm: '2' }),
      node('node-c', 'unavailable', { reachable: false, currentPrimary: null, primaryTerm: null }),
    ])

    expect(availability.available).toBe(false)
    expect(availability.reason).toBe('Cluster routing has no majority agreement')
  })
})
