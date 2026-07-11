import { describe, expect, it } from 'vitest'
import { FieldMergeResolver } from '../conflict/field-merge.js'
import { LWWResolver } from '../conflict/lww.js'
import { PrimaryWinsResolver } from '../conflict/primary-wins.js'
import type { ConflictContext, ReplicationChange } from '../types.js'

function makeChange(overrides: Partial<ReplicationChange> = {}): ReplicationChange {
  return {
    table: 'users',
    operation: 'update',
    rowId: '1',
    primaryKey: { id: 1 },
    hlc: '0000000f4240-0000-nodeA',
    txId: 'tx1',
    nodeId: 'nodeA',
    newData: { id: 1, name: 'Alice' },
    oldData: { id: 1, name: 'Original' },
    ...overrides,
  }
}

describe('LWWResolver', () => {
  const resolver = new LWWResolver()

  it('picks the later HLC (remote wins)', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: null,
      remoteChange: makeChange({ hlc: '0000000f4241-0000-nodeA' }),
      localHlc: '0000000f4240-0000-nodeB',
      remoteHlc: '0000000f4241-0000-nodeA',
    }
    expect(resolver.resolve(ctx).action).toBe('accept_remote')
  })

  it('picks the later HLC (local wins)', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: null,
      remoteChange: makeChange({ hlc: '0000000f4240-0000-nodeA' }),
      localHlc: '0000000f4241-0000-nodeB',
      remoteHlc: '0000000f4240-0000-nodeA',
    }
    expect(resolver.resolve(ctx).action).toBe('keep_local')
  })

  it('breaks ties by comparing nodeId lexicographically', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: makeChange({ nodeId: 'nodeA' }),
      remoteChange: makeChange({ nodeId: 'nodeB' }),
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4240-0000-nodeB',
    }
    expect(resolver.resolve(ctx).action).toBe('accept_remote')
  })

  it('accepts remote when local HLC is null', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: null,
      remoteChange: makeChange(),
      localHlc: null,
      remoteHlc: '0000000f4240-0000-nodeA',
    }
    expect(resolver.resolve(ctx).action).toBe('accept_remote')
  })
})

describe('FieldMergeResolver', () => {
  it('merges non-overlapping changes', async () => {
    const getColumnVersions = async () => {
      return new Map([
        ['name', { hlc: '0000000f4240-0000-nodeA', nodeId: 'nodeA' }],
        ['email', { hlc: '0000000f4240-0000-nodeB', nodeId: 'nodeB' }],
      ])
    }

    const resolver = new FieldMergeResolver(getColumnVersions)

    const localChange = makeChange({
      nodeId: 'nodeA',
      newData: { id: 1, name: 'LocalName', email: 'original@test.com' },
      oldData: { id: 1, name: 'Original', email: 'original@test.com' },
    })

    const remoteChange = makeChange({
      nodeId: 'nodeB',
      hlc: '0000000f4241-0000-nodeB',
      newData: { id: 1, name: 'Original', email: 'remote@test.com' },
      oldData: { id: 1, name: 'Original', email: 'original@test.com' },
    })

    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange,
      remoteChange,
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4241-0000-nodeB',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('merge')
    expect(result.mergedData).toEqual({
      id: 1,
      name: 'LocalName',
      email: 'remote@test.com',
    })
  })

  it('uses per-column HLC for overlapping changes', async () => {
    const getColumnVersions = async () => {
      return new Map([['name', { hlc: '0000000f4240-0000-nodeA', nodeId: 'nodeA' }]])
    }

    const resolver = new FieldMergeResolver(getColumnVersions)

    const localChange = makeChange({
      nodeId: 'nodeA',
      newData: { id: 1, name: 'LocalName' },
      oldData: { id: 1, name: 'Original' },
    })

    const remoteChange = makeChange({
      nodeId: 'nodeB',
      hlc: '0000000f4241-0000-nodeB',
      newData: { id: 1, name: 'RemoteName' },
      oldData: { id: 1, name: 'Original' },
    })

    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange,
      remoteChange,
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4241-0000-nodeB',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('merge')
    expect(result.mergedData?.name).toBe('RemoteName')
  })

  it('falls back to LWW when no column versions are available', async () => {
    const getColumnVersions = async () => new Map<string, { hlc: string; nodeId: string }>()
    const resolver = new FieldMergeResolver(getColumnVersions)

    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: null,
      remoteChange: makeChange({ hlc: '0000000f4241-0000-nodeA' }),
      localHlc: '0000000f4240-0000-nodeB',
      remoteHlc: '0000000f4241-0000-nodeA',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('accept_remote')
  })

  it('resolves an overlapping conflict on an integer column above 2^53 without throwing', async () => {
    const ancestor = 9007199254740993n
    const localBalance = 9007199254740995n
    const remoteBalance = 9007199254740997n

    const getColumnVersions = async () => new Map([['balance', { hlc: '0000000f4240-0000-nodeA', nodeId: 'nodeA' }]])
    const resolver = new FieldMergeResolver(getColumnVersions)

    const ctx: ConflictContext = {
      table: 'accounts',
      rowId: '1',
      localChange: makeChange({
        nodeId: 'nodeA',
        newData: { id: 1, balance: localBalance },
        oldData: { id: 1, balance: ancestor },
      }),
      remoteChange: makeChange({
        nodeId: 'nodeB',
        hlc: '0000000f4241-0000-nodeB',
        newData: { id: 1, balance: remoteBalance },
        oldData: { id: 1, balance: ancestor },
      }),
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4241-0000-nodeB',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('merge')
    expect(result.mergedData?.balance).toBe(remoteBalance)
  })

  it('resolves a conflict on a BLOB column without throwing', async () => {
    const ancestorAvatar = Buffer.from([1, 2, 3])
    const remoteAvatar = Buffer.from([4, 5, 6])
    const localBalance = 9007199254740999n

    const getColumnVersions = async () =>
      new Map([
        ['balance', { hlc: '0000000f4240-0000-nodeA', nodeId: 'nodeA' }],
        ['avatar', { hlc: '0000000f4240-0000-nodeB', nodeId: 'nodeB' }],
      ])
    const resolver = new FieldMergeResolver(getColumnVersions)

    const ctx: ConflictContext = {
      table: 'accounts',
      rowId: '1',
      localChange: makeChange({
        nodeId: 'nodeA',
        newData: { id: 1, balance: localBalance, avatar: ancestorAvatar },
        oldData: { id: 1, balance: 9007199254740993n, avatar: ancestorAvatar },
      }),
      remoteChange: makeChange({
        nodeId: 'nodeB',
        hlc: '0000000f4241-0000-nodeB',
        newData: { id: 1, balance: 9007199254740993n, avatar: remoteAvatar },
        oldData: { id: 1, balance: 9007199254740993n, avatar: ancestorAvatar },
      }),
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4241-0000-nodeB',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('merge')
    expect(result.mergedData?.balance).toBe(localBalance)
    expect(result.mergedData?.avatar).toEqual(remoteAvatar)
  })

  it('does not treat a BigInt equal to a numerically equal number as a change', async () => {
    const getColumnVersions = async () => new Map([['balance', { hlc: '0000000f4240-0000-nodeA', nodeId: 'nodeA' }]])
    const resolver = new FieldMergeResolver(getColumnVersions)

    const ctx: ConflictContext = {
      table: 'accounts',
      rowId: '1',
      localChange: makeChange({
        nodeId: 'nodeA',
        newData: { id: 1, balance: 42n },
        oldData: { id: 1, balance: 42 },
      }),
      remoteChange: makeChange({
        nodeId: 'nodeB',
        hlc: '0000000f4241-0000-nodeB',
        newData: { id: 1, balance: 42 },
        oldData: { id: 1, balance: 42 },
      }),
      localHlc: '0000000f4240-0000-nodeA',
      remoteHlc: '0000000f4241-0000-nodeB',
    }

    const result = await resolver.resolve(ctx)
    expect(result.action).toBe('keep_local')
  })
})

describe('PrimaryWinsResolver', () => {
  const resolver = new PrimaryWinsResolver('primaryNode')

  it('accepts remote when remote is primary', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: makeChange({ nodeId: 'replicaNode' }),
      remoteChange: makeChange({ nodeId: 'primaryNode' }),
      localHlc: '0000000f4241-0000-replicaNode',
      remoteHlc: '0000000f4240-0000-primaryNode',
    }
    expect(resolver.resolve(ctx).action).toBe('accept_remote')
  })

  it('keeps local when local is primary', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: makeChange({ nodeId: 'primaryNode' }),
      remoteChange: makeChange({ nodeId: 'replicaNode' }),
      localHlc: '0000000f4240-0000-primaryNode',
      remoteHlc: '0000000f4241-0000-replicaNode',
    }
    expect(resolver.resolve(ctx).action).toBe('keep_local')
  })

  it('falls back to LWW when neither is primary', () => {
    const ctx: ConflictContext = {
      table: 'users',
      rowId: '1',
      localChange: makeChange({ nodeId: 'peerA' }),
      remoteChange: makeChange({ nodeId: 'peerB', hlc: '0000000f4241-0000-peerB' }),
      localHlc: '0000000f4240-0000-peerA',
      remoteHlc: '0000000f4241-0000-peerB',
    }
    expect(resolver.resolve(ctx).action).toBe('accept_remote')
  })
})
