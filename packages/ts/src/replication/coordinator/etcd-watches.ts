import type { IWatchResponse, WatchBuilder, Watcher } from 'etcd3'
import { CoordinatorError } from '../errors.js'
import type { CoordinatorWatchDisposer } from './types.js'

const WATCH_REOPEN_RETRY_MS = 1_000

/**
 * Holds an etcd watcher that has connected, the ID of the etcd cluster that answered its create request, and what the
 * watcher has reported since then.
 *
 * @internal
 */
export interface OpenedEtcdWatch {
  /** Holds the connected watcher. */
  watcher: Watcher
  /** Holds the ID of the etcd cluster that answered the watcher's first create request. */
  clusterId: string
  /** Holds the ID of the etcd cluster that answered the watcher's latest create request. */
  connectedClusterId: string
  /** Is true once etcd has cancelled the watcher. */
  cancelledByEtcd: boolean
}

/**
 * Tracks every etcd watcher that a coordinator keeps open, so that closing the coordinator stops each watch, including
 * a watch that is reopening on another etcd cluster at that moment.
 *
 * @internal
 */
export class EtcdWatchers {
  private readonly open = new Set<Watcher>()
  private closedFlag = false

  /** Is true once {@link EtcdWatchers.cancelAll} has run. */
  get closed(): boolean {
    return this.closedFlag
  }

  /** Records a watcher, or cancels it at once when the coordinator has closed. */
  async add(watcher: Watcher): Promise<void> {
    if (this.closedFlag) {
      await watcher.cancel().catch(() => undefined)
      return
    }
    this.open.add(watcher)
  }

  /** Cancels one watcher and stops tracking it. */
  async cancel(watcher: Watcher): Promise<void> {
    this.open.delete(watcher)
    await watcher.cancel().catch(() => undefined)
  }

  /** Stops tracking a watcher that etcd has already cancelled, which takes no cancel request. */
  forget(watcher: Watcher): void {
    this.open.delete(watcher)
  }

  /** Cancels every watcher, and cancels each watcher added after this call. */
  async cancelAll(): Promise<void> {
    this.closedFlag = true
    const cancels = [...this.open].map(watcher => watcher.cancel())
    this.open.clear()
    await Promise.allSettled(cancels)
  }
}

/**
 * Returns a watcher once etcd has answered its create request, along with the ID of the etcd cluster that answered.
 * The result records every later reconnection and the cancellation from the moment that the watcher exists, so that
 * {@link followEtcdCluster} sees a change that happens while the caller reads the state that the watch starts from.
 *
 * @param builder - The watch to create.
 * @returns The connected watcher and the etcd cluster ID from the create response.
 * @throws Error when etcd refuses the watch.
 *
 * @internal
 */
export async function connectEtcdWatch(builder: WatchBuilder): Promise<OpenedEtcdWatch> {
  const watcher = builder.watcher()
  const opened: OpenedEtcdWatch = { watcher, clusterId: '', connectedClusterId: '', cancelledByEtcd: false }
  watcher.on('connected', (response: IWatchResponse) => {
    opened.connectedClusterId = response.header.cluster_id
  })
  watcher.on('error', () => {
    opened.cancelledByEtcd = true
  })
  opened.clusterId = await new Promise<string>((resolve, reject) => {
    const onConnected = (response: IWatchResponse) => {
      watcher.off('error', onError)
      resolve(response.header.cluster_id)
    }
    const onError = (err: Error) => {
      watcher.off('connected', onConnected)
      reject(err)
    }
    watcher.once('connected', onConnected)
    watcher.once('error', onError)
  })
  return opened
}

/**
 * Checks that etcd answered a read from the same cluster as a watch, so that the state that a watch starts from and
 * the changes that it reports come from one etcd cluster.
 *
 * @param opened - The watch that the read belongs to.
 * @param readClusterId - The etcd cluster ID from the read's response header.
 * @throws CoordinatorError when the read came from another etcd cluster.
 *
 * @internal
 */
export function assertReadFromWatchedCluster(opened: OpenedEtcdWatch, readClusterId: string): void {
  if (readClusterId !== opened.clusterId) {
    throw new CoordinatorError('etcd answered a read from another cluster than the one that the watch connected to', {
      watchClusterId: opened.clusterId,
      readClusterId,
    })
  }
}

function isStale(opened: OpenedEtcdWatch): boolean {
  return opened.cancelledByEtcd || opened.connectedClusterId !== opened.clusterId
}

/**
 * Returns a function that stops a watch which follows whichever etcd cluster the client reaches. The function calls
 * `open` again in two cases. When the watcher reconnects to an etcd cluster with another ID, such as one that an
 * operator restored from a snapshot or built anew, the watcher would otherwise wait for revisions of the former
 * cluster that the new cluster may never reach, so the function cancels it first. When etcd cancels the watcher, as
 * it does once a restore marks the watcher's revisions compacted, the watcher has already stopped.
 *
 * @param open - Opens the watch and reads the state that it starts from. It receives true when it reopens the watch,
 * so that it can report the state that etcd holds at that point.
 * @param watchers - The coordinator's open watchers.
 * @param onWatcherError - Receives each failure to reopen the watch, after which the function tries again.
 * @returns A function that stops the watch.
 * @throws Error when the first call to `open` fails.
 *
 * @internal
 */
export async function followEtcdCluster(
  open: (reopening: boolean) => Promise<OpenedEtcdWatch>,
  watchers: EtcdWatchers,
  onWatcherError: ((error: Error) => void) | undefined,
): Promise<CoordinatorWatchDisposer> {
  let current: OpenedEtcdWatch | null = null
  let stopped = false
  let reopening: Promise<void> | null = null

  const release = async (opened: OpenedEtcdWatch): Promise<void> => {
    if (opened.cancelledByEtcd) watchers.forget(opened.watcher)
    else await watchers.cancel(opened.watcher)
  }

  const reopen = async (previous: OpenedEtcdWatch): Promise<void> => {
    current = null
    await release(previous)
    while (!stopped && !watchers.closed) {
      let next: OpenedEtcdWatch
      try {
        next = await open(true)
      } catch (err: unknown) {
        onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
        await new Promise(resolve => setTimeout(resolve, WATCH_REOPEN_RETRY_MS))
        continue
      }
      await follow(next)
      if (!isStale(next)) {
        current = next
        return
      }
      await release(next)
    }
  }

  const startReopen = (opened: OpenedEtcdWatch): void => {
    if (stopped || reopening || opened !== current || !isStale(opened)) return
    reopening = reopen(opened).finally(() => {
      reopening = null
      if (current) startReopen(current)
    })
  }

  const follow = async (opened: OpenedEtcdWatch): Promise<void> => {
    await watchers.add(opened.watcher)
    const onChange = () => startReopen(opened)
    opened.watcher.on('connected', onChange)
    opened.watcher.on('error', onChange)
  }

  const first = await open(false)
  await follow(first)
  current = first
  startReopen(first)
  return async () => {
    stopped = true
    await reopening
    const last = current
    current = null
    if (last) await release(last)
  }
}
