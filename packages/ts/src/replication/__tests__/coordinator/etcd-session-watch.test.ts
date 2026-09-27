import { EventEmitter } from 'node:events'
import type { Namespace, Watcher } from 'etcd3'
import { describe, expect, it } from 'vitest'
import { watchEtcdNodeSessions } from '../../coordinator/etcd-session-watch.js'

const CLUSTER_ID = 'cluster-a'
const PREFIX = 'clusters/cluster-a/nodes/'

class FakeWatcher extends EventEmitter {
  cancelled = false

  async cancel(): Promise<void> {
    this.cancelled = true
  }

  put(nodeId: string): void {
    this.emit('put', { key: Buffer.from(`${PREFIX}${nodeId}`) })
  }

  remove(nodeId: string): void {
    this.emit('delete', { key: Buffer.from(`${PREFIX}${nodeId}`) })
  }
}

function fakeNamespace(watcher: FakeWatcher, readKeys: () => Promise<string[]>): Namespace {
  return {
    watch: () => ({ prefix: () => ({ create: async () => watcher }) }),
    getAll: () => ({ prefix: () => ({ keys: readKeys }) }),
  } as unknown as Namespace
}

describe('the etcd watch on node sessions', () => {
  it('reports the nodes already registered, then every later change', async () => {
    const watcher = new FakeWatcher()
    const namespace = fakeNamespace(watcher, async () => [`${PREFIX}node-a`, `${PREFIX}node-b`])
    const seen: string[][] = []

    const stop = await watchEtcdNodeSessions(namespace, CLUSTER_ID, live => seen.push([...live]), new Set<Watcher>())
    watcher.put('node-c')
    watcher.remove('node-a')

    expect(seen).toEqual([
      ['node-a', 'node-b'],
      ['node-a', 'node-b', 'node-c'],
      ['node-b', 'node-c'],
    ])
    await stop()
    expect(watcher.cancelled).toBe(true)
  })

  it('leaves out a node whose session lapses while the first read is in flight', async () => {
    const watcher = new FakeWatcher()
    const namespace = fakeNamespace(watcher, async () => {
      watcher.remove('node-a')
      return [`${PREFIX}node-a`, `${PREFIX}node-b`]
    })
    const seen: string[][] = []

    await watchEtcdNodeSessions(namespace, CLUSTER_ID, live => seen.push([...live]), new Set<Watcher>())

    expect(seen).toEqual([['node-b']])
  })

  it('stops the watch when the first read fails', async () => {
    const watcher = new FakeWatcher()
    const watchers = new Set<Watcher>()
    const namespace = fakeNamespace(watcher, async () => {
      throw new Error('etcd is unreachable')
    })

    await expect(watchEtcdNodeSessions(namespace, CLUSTER_ID, () => {}, watchers)).rejects.toThrow(
      'etcd is unreachable',
    )
    expect(watcher.cancelled).toBe(true)
    expect(watchers.size).toBe(0)
  })
})
