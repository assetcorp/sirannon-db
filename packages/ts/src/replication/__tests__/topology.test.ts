import { describe, expect, it } from 'vitest'
import { MultiPrimaryTopology } from '../topology/multi-primary.js'
import { PrimaryReplicaTopology } from '../topology/primary-replica.js'

describe('PrimaryReplicaTopology', () => {
  describe('primary role', () => {
    const topology = new PrimaryReplicaTopology('primary')

    it('can write', () => {
      expect(topology.canWrite()).toBe(true)
    })

    it('replicates to replicas', () => {
      expect(topology.shouldReplicateTo('peer1', 'replica')).toBe(true)
    })

    it('does not replicate to other primaries', () => {
      expect(topology.shouldReplicateTo('peer1', 'primary')).toBe(false)
    })

    it('does not accept from anyone', () => {
      expect(topology.shouldAcceptFrom('peer1', 'replica')).toBe(false)
      expect(topology.shouldAcceptFrom('peer1', 'primary')).toBe(false)
    })

    it('does not require conflict resolution', () => {
      expect(topology.requiresConflictResolution()).toBe(false)
    })
  })

  describe('replica role', () => {
    const topology = new PrimaryReplicaTopology('replica')

    it('cannot write', () => {
      expect(topology.canWrite()).toBe(false)
    })

    it('does not replicate to anyone', () => {
      expect(topology.shouldReplicateTo('peer1', 'primary')).toBe(false)
      expect(topology.shouldReplicateTo('peer1', 'replica')).toBe(false)
    })

    it('accepts from primary only', () => {
      expect(topology.shouldAcceptFrom('peer1', 'primary')).toBe(true)
      expect(topology.shouldAcceptFrom('peer1', 'replica')).toBe(false)
    })

    it('does not require conflict resolution', () => {
      expect(topology.requiresConflictResolution()).toBe(false)
    })
  })
})

describe('MultiPrimaryTopology', () => {
  const topology = new MultiPrimaryTopology()

  it('can write', () => {
    expect(topology.canWrite()).toBe(true)
  })

  it('replicates to all peer roles', () => {
    expect(topology.shouldReplicateTo('peer1', 'peer')).toBe(true)
    expect(topology.shouldReplicateTo('peer1', 'primary')).toBe(true)
    expect(topology.shouldReplicateTo('peer1', 'replica')).toBe(true)
  })

  it('accepts from all peer roles', () => {
    expect(topology.shouldAcceptFrom('peer1', 'peer')).toBe(true)
    expect(topology.shouldAcceptFrom('peer1', 'primary')).toBe(true)
    expect(topology.shouldAcceptFrom('peer1', 'replica')).toBe(true)
  })

  it('requires conflict resolution', () => {
    expect(topology.requiresConflictResolution()).toBe(true)
  })
})
