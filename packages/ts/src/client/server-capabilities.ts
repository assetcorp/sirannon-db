import { SQL_QUERY_CAPABILITY } from '../server/capabilities.js'
import { DEFAULT_HTTP_REQUEST_TIMEOUT_MS } from './http-json.js'
import type { CapabilityReport } from './sync-capabilities.js'
import { fetchCapabilityReport } from './sync-capabilities.js'
import { RemoteError } from './types.js'

/**
 * Message a client raises when the server accepts no SQL over the network.
 *
 * @internal
 */
export const SQL_REFUSED_MESSAGE =
  'This server does not accept SQL over the network. Call a registered operation by name, or start the server with acceptSql: true.'

export const SQL_UNCONFIRMED_MESSAGE =
  'GET /capabilities returned 404, so this client cannot confirm that the server accepts SQL and refuses to send it. Check that the URL reaches a sirannon-db server and that any proxy in front of it forwards /capabilities.'

/**
 * Capability questions a remote database asks before it sends a statement.
 *
 * @public
 */
export interface ServerCapabilityCheck {
  /** Throws when the server accepts no SQL over the network, so a client refuses to send any. */
  assertSqlAccepted(): Promise<void>
  /** Reads the digest of the server's operation registry, or undefined when the server announces none. */
  registryDigest(refresh?: boolean): Promise<string | undefined>
}

/**
 * Reads and caches the capability report a server announces.
 *
 * @internal
 */
export class ServerCapabilities implements ServerCapabilityCheck {
  private pending: Promise<CapabilityReport> | null = null

  constructor(
    private readonly resolveUrl: () => string | Promise<string>,
    private readonly headers: Record<string, string> | undefined,
    private readonly requestTimeoutMs: number = DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
  ) {}

  private async read(refresh = false): Promise<CapabilityReport> {
    if (refresh) this.pending = null
    if (this.pending === null) {
      const request = this.fetch()
      this.pending = request
      request.catch(() => {
        if (this.pending === request) this.pending = null
      })
    }
    return this.pending
  }

  async registryDigest(refresh = false): Promise<string | undefined> {
    try {
      return (await this.read(refresh)).registryDigest
    } catch (err) {
      if (isMissingEndpoint(err)) return undefined
      throw err
    }
  }

  async assertSqlAccepted(): Promise<void> {
    let report: CapabilityReport
    try {
      report = await this.read()
    } catch (err) {
      if (isMissingEndpoint(err)) throw new RemoteError('SQL_NOT_ACCEPTED', SQL_UNCONFIRMED_MESSAGE)
      throw err
    }

    if (!report.capabilities.includes(SQL_QUERY_CAPABILITY)) {
      throw new RemoteError('SQL_NOT_ACCEPTED', SQL_REFUSED_MESSAGE)
    }
  }

  private async fetch(): Promise<CapabilityReport> {
    const url = await this.resolveUrl()
    return fetchCapabilityReport({ url, headers: this.headers, requestTimeoutMs: this.requestTimeoutMs })
  }
}

function isMissingEndpoint(err: unknown): boolean {
  return err instanceof RemoteError && err.code === 'NOT_FOUND'
}
