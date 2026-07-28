import type { ClientOptions } from '../core/types.js'
import { createEndpointTransport, DatabaseClient } from './client-base.js'
import { toBaseUrl } from './endpoint-urls.js'
import { RemoteError, type Transport } from './types.js'

const TOPOLOGY_KEYS = ['endpoints', 'primary', 'replicas', 'readPreference', 'discovery', 'readConcern'] as const

function normaliseClientUrl(url: string): string {
  const value: unknown = url
  if (typeof value === 'string') return toBaseUrl(value)

  const named = typeof value === 'object' && value !== null ? TOPOLOGY_KEYS.find(key => key in value) : undefined
  const subject = named === undefined ? 'Topology-aware routing' : `The '${named}' option`
  throw new RemoteError(
    'INVALID_ARGUMENT',
    `SirannonClient connects to a single server URL and never routes between nodes. ${subject} moved to the '@delali/sirannon-db/client/topology' entry point, which a browser bundle must not import.`,
  )
}

export class SirannonClient extends DatabaseClient {
  private readonly baseUrl: string

  constructor(url: string, options?: ClientOptions) {
    super(options)
    this.baseUrl = normaliseClientUrl(url)
  }

  protected createTransport(databaseId: string): Transport {
    return createEndpointTransport(this.settings, this.baseUrl, databaseId)
  }

  protected resolveServerUrl(): string {
    return this.baseUrl
  }
}
