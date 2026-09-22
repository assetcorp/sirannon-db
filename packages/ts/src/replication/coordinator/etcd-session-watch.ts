import type { Namespace, Watcher } from 'etcd3'
import { nodeSessionPrefix } from './etcd-connection.js'
import type { CoordinatorWatchDisposer, NodeSessionWatcher } from './types.js'

function nodeIdOf(key: string, prefix: string): string {
  return decodeURIComponent(key.slice(prefix.length))
}

export async function watchEtcdNodeSessions(
  namespace: Namespace,
  clusterId: string,
  watcher: NodeSessionWatcher,
  watchers: Set<Watcher>,
  onWatcherError?: (error: Error) => void,
): Promise<CoordinatorWatchDisposer> {
  const prefix = nodeSessionPrefix(clusterId)
  const live = new Set<string>()
  const goneWhileReading = new Set<string>()
  let reading = true

  const report = () => {
    try {
      watcher([...live])
    } catch (err: unknown) {
      onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    }
  }

  const etcdWatcher = await namespace.watch().prefix(prefix).create()
  watchers.add(etcdWatcher)

  etcdWatcher.on('put', kv => {
    live.add(nodeIdOf(kv.key.toString('utf8'), prefix))
    if (!reading) report()
  })
  etcdWatcher.on('delete', kv => {
    const nodeId = nodeIdOf(kv.key.toString('utf8'), prefix)
    live.delete(nodeId)
    if (reading) {
      goneWhileReading.add(nodeId)
      return
    }
    report()
  })
  etcdWatcher.on('error', err => {
    onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
  })

  try {
    for (const key of await namespace.getAll().prefix(prefix).keys()) {
      const nodeId = nodeIdOf(key, prefix)
      if (!goneWhileReading.has(nodeId)) {
        live.add(nodeId)
      }
    }
  } catch (err: unknown) {
    watchers.delete(etcdWatcher)
    await etcdWatcher.cancel()
    throw err
  }

  reading = false
  report()

  return async () => {
    watchers.delete(etcdWatcher)
    await etcdWatcher.cancel()
  }
}
