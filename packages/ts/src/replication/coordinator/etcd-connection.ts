import type { Etcd3, IOptions, Namespace } from 'etcd3'
import type { EtcdGroupStore } from './etcd-group-store.js'
import { assertNonEmpty } from './group-rules.js'

/**
 * Sets the etcd endpoints that the etcd coordinator connects to, the key prefix that it stores group state under, and
 * how it authenticates.
 *
 * @public
 */
export interface EtcdClusterCoordinatorOptions {
  /**
   * Lists the etcd endpoints to connect to. The coordinator accepts only https endpoints unless `allowInsecure` is true.
   */
  hosts: string | string[]
  /**
   * Sets the prefix that this coordinator puts in front of every key that it writes, so that several deployments can
   * share one etcd.
   */
  keyPrefix: string
  /**
   * Holds the TLS material, which the coordinator requires unless `allowInsecure` is true.
   */
  credentials?: IOptions['credentials']
  /**
   * Holds a username and password, which the coordinator accepts in place of a client certificate.
   */
  auth?: IOptions['auth']
  /**
   * Holds options that the coordinator passes unchanged to the underlying gRPC channel.
   */
  grpcOptions?: IOptions['grpcOptions']
  /**
   * Sets how many milliseconds the client waits to connect to etcd.
   */
  dialTimeoutMs?: number
  /**
   * Sets how many milliseconds a single etcd call can take before its deadline expires.
   */
  defaultCallTimeoutMs?: number
  /**
   * Lets the coordinator connect to plain http endpoints without credentials. Use it only in tests.
   */
  allowInsecure?: boolean
  /**
   * Receives each error that an etcd watch reports or that a watch callback throws, and each lost lease, so that you
   * can log it or raise an alert.
   */
  onWatcherError?: (error: Error) => void
}

export function assertEtcdOptions(options: EtcdClusterCoordinatorOptions): void {
  const hosts = Array.isArray(options.hosts) ? options.hosts : [options.hosts]
  if (hosts.length === 0) {
    throw new TypeError('hosts must contain at least one etcd endpoint')
  }
  for (const host of hosts) {
    assertNonEmpty(host, 'hosts entry')
    if (!options.allowInsecure && !host.startsWith('https://')) {
      throw new TypeError('production coordinator access requires https etcd endpoints')
    }
  }
  assertNonEmpty(options.keyPrefix, 'keyPrefix')
  if (!options.allowInsecure && !options.credentials) {
    throw new TypeError('production coordinator access requires TLS credentials')
  }
  const hasMtlsIdentity = Boolean(options.credentials?.privateKey && options.credentials.certChain)
  const hasPasswordAuth = Boolean(options.auth?.username && options.auth.password)
  if (!options.allowInsecure && !hasMtlsIdentity && !hasPasswordAuth) {
    throw new TypeError('production coordinator access requires an authenticated Sirannon identity')
  }
}

export function toEtcdOptions(options: EtcdClusterCoordinatorOptions): IOptions {
  const defaultCallTimeoutMs = options.defaultCallTimeoutMs
  const defaultCallOptions = defaultCallTimeoutMs ? () => ({ deadline: Date.now() + defaultCallTimeoutMs }) : undefined
  return {
    hosts: options.hosts,
    credentials: options.credentials,
    auth: options.auth,
    grpcOptions: options.grpcOptions,
    dialTimeout: options.dialTimeoutMs,
    defaultCallOptions,
  }
}

export interface EtcdConnection {
  client: Etcd3
  namespace: Namespace
  groups: EtcdGroupStore
}

export function normaliseKeyPrefix(prefix: string): string {
  const trimmed = prefix.replace(/^\/+/, '').replace(/\/+$/, '')
  if (trimmed.length === 0) {
    throw new TypeError('keyPrefix must not resolve to the etcd root')
  }
  return `${trimmed}/`
}

export function controllerLeaseKey(clusterId: string): string {
  return `clusters/${encodeKey(clusterId)}/controller`
}

export function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `${nodeSessionPrefix(clusterId)}${encodeKey(nodeId)}`
}

export function nodeSessionPrefix(clusterId: string): string {
  return `clusters/${encodeKey(clusterId)}/nodes/`
}

export function replicationGroupKey(clusterId: string, groupId: string): string {
  return `clusters/${encodeKey(clusterId)}/groups/${encodeKey(groupId)}`
}

export function ttlMsToSeconds(ttlMs: number): number {
  return Math.max(1, Math.ceil(ttlMs / 1000))
}

function encodeKey(value: string): string {
  return encodeURIComponent(value)
}
