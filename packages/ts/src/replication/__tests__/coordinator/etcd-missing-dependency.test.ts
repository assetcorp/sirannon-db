import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => {
  vi.doUnmock('etcd3')
  vi.resetModules()
})

describe('EtcdClusterCoordinator without etcd3', () => {
  it('builds the coordinator and fails its first call with COORDINATOR_DEPENDENCY_MISSING', async () => {
    vi.doMock('etcd3', () => {
      throw new Error("Cannot find package 'etcd3'")
    })
    vi.resetModules()

    const { createEtcdCoordinator } = await import('../../coordinator/etcd.js')
    const coordinator = createEtcdCoordinator({
      hosts: 'http://127.0.0.1:2379',
      keyPrefix: 'sirannon/cluster-a',
      allowInsecure: true,
    })

    await expect(coordinator.getReplicationGroupState('commerce-production', 'orders')).rejects.toMatchObject({
      code: 'COORDINATOR_DEPENDENCY_MISSING',
      message: expect.stringContaining('`pnpm add -E etcd3`'),
    })
    await expect(coordinator.close()).resolves.toBeUndefined()
  })
})
