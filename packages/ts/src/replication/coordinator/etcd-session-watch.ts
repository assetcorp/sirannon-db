import type { Namespace } from 'etcd3'
import { nodeSessionPrefix } from './etcd-connection.js'
import {
  assertReadFromWatchedCluster,
  connectEtcdWatch,
  type EtcdWatchers,
  followEtcdCluster,
  type OpenedEtcdWatch,
} from './etcd-watches.js'
import type { CoordinatorWatchDisposer, NodeSessionWatcher } from './types.js'

function nodeIdOf(key: string, prefix: string): string {
  return decodeURIComponent(key.slice(prefix.length))
}

export async function watchEtcdNodeSessions(
  namespace: Namespace,
  clusterId: string,
  watcher: NodeSessionWatcher,
  watchers: EtcdWatchers,
  onWatcherError?: (error: Error) => void,
): Promise<CoordinatorWatchDisposer> {
  const prefix = nodeSessionPrefix(clusterId)
  const report = (live: Set<string>) => {
    try {
      watcher([...live])
    } catch (err: unknown) {
      onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    }
  }

  const open = async (): Promise<OpenedEtcdWatch> => {
    const live = new Set<string>()
    const goneWhileReading = new Set<string>()
    let reading = true
    const opened = await connectEtcdWatch(namespace.watch().prefix(prefix))
    const etcdWatcher = opened.watcher

    etcdWatcher.on('put', kv => {
      live.add(nodeIdOf(kv.key.toString('utf8'), prefix))
      if (!reading) report(live)
    })
    etcdWatcher.on('delete', kv => {
      const nodeId = nodeIdOf(kv.key.toString('utf8'), prefix)
      live.delete(nodeId)
      if (reading) {
        goneWhileReading.add(nodeId)
        return
      }
      report(live)
    })
    etcdWatcher.on('error', err => {
      onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    })

    try {
      const response = await namespace.getAll().prefix(prefix).exec()
      assertReadFromWatchedCluster(opened, response.header.cluster_id)
      for (const kv of response.kvs) {
        const nodeId = nodeIdOf(kv.key.toString('utf8'), prefix)
        if (!goneWhileReading.has(nodeId)) {
          live.add(nodeId)
        }
      }
    } catch (err: unknown) {
      await etcdWatcher.cancel().catch(() => undefined)
      throw err
    }

    reading = false
    report(live)
    return opened
  }

  return followEtcdCluster(open, watchers, onWatcherError)
}
