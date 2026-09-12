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

/**
 * Connects to one sirannon-db server and hands out a {@link RemoteDatabase} per database.
 *
 * @public
 */
export class SirannonClient extends DatabaseClient {
  private readonly baseUrl: string

  constructor(url: string, options?: ClientOptions) {
    super(options)
    this.baseUrl = normaliseClientUrl(url)
  }

  /**
   * Builds the transport a database's requests travel over.
   *
   * @param databaseId - Identifier of the database.
   * @returns A transport bound to that database.
   */
  protected createTransport(databaseId: string): Transport {
    return createEndpointTransport(this.settings, this.baseUrl, databaseId)
  }

  /**
   * Returns the address requests are sent to.
   *
   * @returns The server's base address.
   */
  protected resolveServerUrl(): string {
    return this.baseUrl
  }
}
