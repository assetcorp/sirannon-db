import { describe, expect, it } from 'vitest'
import { parseClusterStatus } from '../cluster-status.js'
import { RemoteError } from '../types.js'

const DATABASE_ID = 'entitlements'

function body(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    databaseId: DATABASE_ID,
    role: 'primary',
    health: 'healthy',
    healthReason: 'in-sync',
    currentPrimary: { nodeId: 'node-a', endpoint: 'http://127.0.0.1:7301' },
    primaryTerm: '7',
    readEndpoints: [
      { nodeId: 'node-b', endpoint: 'http://127.0.0.1:7302', readConcerns: ['local'] },
      { nodeId: 'node-c', endpoint: 'http://127.0.0.1:7303', readConcerns: ['local'] },
    ],
    replicationGroupId: 'entitlements',
    ...overrides,
  }
}

describe('parseClusterStatus', () => {
  it('reads every field a cluster dashboard shows', () => {
    const status = parseClusterStatus(body(), DATABASE_ID)

    expect(status.databaseId).toBe(DATABASE_ID)
    expect(status.role).toBe('primary')
    expect(status.health).toBe('healthy')
    expect(status.healthReason).toBe('in-sync')
    expect(status.currentPrimary?.nodeId).toBe('node-a')
    expect(status.readEndpoints).toHaveLength(2)
  })

  it('reads primaryTerm as a bigint, so a term beyond the safe integer range survives', () => {
    const status = parseClusterStatus(body({ primaryTerm: '9007199254740993' }), DATABASE_ID)

    expect(status.primaryTerm).toBe(9007199254740993n)
  })

  it('reads a group that currently names no primary', () => {
    const status = parseClusterStatus(body({ currentPrimary: null }), DATABASE_ID)

    expect(status.currentPrimary).toBeNull()
  })

  it('refuses a response for another database', () => {
    expect(() => parseClusterStatus(body(), 'other')).toThrow(RemoteError)
  })

  it('refuses a health value the engine never reports', () => {
    expect(() => parseClusterStatus(body({ health: 'exhausted' }), DATABASE_ID)).toThrow(RemoteError)
  })

  it('refuses a healthReason value the engine never reports', () => {
    expect(() => parseClusterStatus(body({ healthReason: 'tired' }), DATABASE_ID)).toThrow(RemoteError)
  })

  it('refuses a body that is not an object', () => {
    expect(() => parseClusterStatus('nope', DATABASE_ID)).toThrow(RemoteError)
  })
})
