import type { IOptions } from 'etcd3'
import { assertNonEmpty } from './group-rules.js'

export interface EtcdClusterCoordinatorOptions {
  hosts: string | string[]
  keyPrefix: string
  credentials?: IOptions['credentials']
  auth?: IOptions['auth']
  grpcOptions?: IOptions['grpcOptions']
  dialTimeoutMs?: number
  defaultCallTimeoutMs?: number
  allowInsecure?: boolean
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
  return `clusters/${encodeKey(clusterId)}/nodes/${encodeKey(nodeId)}`
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
