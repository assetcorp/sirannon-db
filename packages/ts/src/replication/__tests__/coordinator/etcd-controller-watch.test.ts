import { EventEmitter } from 'node:events'
import type { Namespace, Watcher } from 'etcd3'
import { describe, expect, it } from 'vitest'
import { watchEtcdControllerLease } from '../../coordinator/etcd-controller-lease.js'
import type { CoordinatorLease } from '../../coordinator/types.js'

const CLUSTER_ID = 'cluster-a'

function leaseValue(holderId: string, expiresAtMs: number): string {
  return JSON.stringify({
    id: `lease-${holderId}`,
    kind: 'controller',
    clusterId: CLUSTER_ID,
    holderId,
    ttlMs: 10_000,
    grantedAtMs: expiresAtMs - 10_000,
    expiresAtMs,
  })
}

class FakeWatcher extends EventEmitter {
  cancelled = false

  async cancel(): Promise<void> {
    this.cancelled = true
  }

  put(holderId: string, expiresAtMs: number): void {
    this.emit('put', { value: Buffer.from(leaseValue(holderId, expiresAtMs)) })
  }

  remove(): void {
    this.emit('delete', {})
  }
}

function fakeNamespace(watcher: FakeWatcher, readValue: () => Promise<string | null>): Namespace {
  return {
    watch: () => ({ key: () => ({ create: async () => watcher }) }),
    get: () => ({ string: readValue }),
  } as unknown as Namespace
}

function holderIds(seen: (CoordinatorLease | null)[]): (string | null)[] {
  return seen.map(lease => lease?.holderId ?? null)
}

describe('the etcd watch on the controller lease', () => {
  it('reports the holder it reads, then every later change', async () => {
    const watcher = new FakeWatcher()
    const namespace = fakeNamespace(watcher, async () => leaseValue('node-a', 11_000))
    const seen: (CoordinatorLease | null)[] = []

    const stop = await watchEtcdControllerLease(namespace, CLUSTER_ID, lease => seen.push(lease), new Set<Watcher>())
    watcher.remove()
    watcher.put('node-b', 21_000)

    expect(holderIds(seen)).toEqual(['node-a', null, 'node-b'])
    expect(seen[2]?.expiresAtMs).toBe(21_000)
    await stop()
    expect(watcher.cancelled).toBe(true)
  })

  it('keeps the holder that arrives while the first read is in flight', async () => {
    const watcher = new FakeWatcher()
    const namespace = fakeNamespace(watcher, async () => {
      watcher.remove()
      return leaseValue('node-a', 11_000)
    })
    const seen: (CoordinatorLease | null)[] = []

    await watchEtcdControllerLease(namespace, CLUSTER_ID, lease => seen.push(lease), new Set<Watcher>())

    expect(holderIds(seen)).toEqual([null])
  })

  it('stops the watch when the first read fails', async () => {
    const watcher = new FakeWatcher()
    const watchers = new Set<Watcher>()
    const namespace = fakeNamespace(watcher, async () => {
      throw new Error('etcd is unreachable')
    })

    await expect(watchEtcdControllerLease(namespace, CLUSTER_ID, () => {}, watchers)).rejects.toThrow(
      'etcd is unreachable',
    )
    expect(watcher.cancelled).toBe(true)
    expect(watchers.size).toBe(0)
  })
})
