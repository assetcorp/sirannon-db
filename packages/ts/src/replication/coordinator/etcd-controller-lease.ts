import type { Namespace, Watcher } from 'etcd3'
import { parseLease, serializeLease } from './etcd-codec.js'
import { controllerLeaseKey, ttlMsToSeconds } from './etcd-connection.js'
import { type EtcdLeaseRegistry, readLeaseAt, revokeLeaseQuietly } from './etcd-lease-registry.js'
import { cloneMetadata } from './group-rules.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  ControllerLeaseWatcher,
  CoordinatorLease,
  CoordinatorWatchDisposer,
} from './types.js'

export async function acquireEtcdControllerLease(
  namespace: Namespace,
  leases: EtcdLeaseRegistry,
  input: AcquireControllerLeaseInput,
): Promise<AcquireControllerLeaseResult> {
  const key = controllerLeaseKey(input.clusterId)
  const lease = namespace.lease(ttlMsToSeconds(input.ttlMs))
  const leaseId = await lease.grant()
  const grantedAtMs = Date.now()
  const value = serializeLease({
    id: leaseId,
    kind: 'controller',
    clusterId: input.clusterId,
    holderId: input.holderId,
    ttlMs: input.ttlMs,
    grantedAtMs,
    expiresAtMs: grantedAtMs + input.ttlMs,
    metadata: cloneMetadata(input.metadata),
  })

  const result = await namespace
    .if(key, 'Create', '==', 0)
    .then(namespace.put(key).value(value).lease(leaseId))
    .commit()

  if (!result.succeeded) {
    await revokeLeaseQuietly(lease)
    return { acquired: false, lease: await readLeaseAt(namespace, key) }
  }

  leases.track(lease, {
    leaseId,
    key,
    ttlMs: input.ttlMs,
    ttlSeconds: ttlMsToSeconds(input.ttlMs),
    kind: 'controller',
    clusterId: input.clusterId,
    holderId: input.holderId,
    metadata: cloneMetadata(input.metadata),
  })

  return { acquired: true, lease: parseLease(value) }
}

export async function watchEtcdControllerLease(
  namespace: Namespace,
  clusterId: string,
  watcher: ControllerLeaseWatcher,
  watchers: Set<Watcher>,
  onWatcherError?: (error: Error) => void,
): Promise<CoordinatorWatchDisposer> {
  const key = controllerLeaseKey(clusterId)
  let holder: CoordinatorLease | null = null
  let reading = true
  let changedWhileReading = false

  const report = () => {
    try {
      watcher(holder)
    } catch (err: unknown) {
      onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    }
  }

  const record = (next: CoordinatorLease | null) => {
    holder = next
    if (reading) {
      changedWhileReading = true
      return
    }
    report()
  }

  const etcdWatcher = await namespace.watch().key(key).create()
  watchers.add(etcdWatcher)

  etcdWatcher.on('put', kv => {
    record(parseLease(kv.value.toString('utf8')))
  })
  etcdWatcher.on('delete', () => {
    record(null)
  })
  etcdWatcher.on('error', err => {
    onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
  })

  try {
    const value = await namespace.get(key).string()
    if (!changedWhileReading) {
      holder = value ? parseLease(value) : null
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
