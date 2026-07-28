import { SQL_QUERY_CAPABILITY } from '../server/capabilities.js'
import { DEFAULT_HTTP_REQUEST_TIMEOUT_MS } from './http-json.js'
import type { CapabilityReport } from './sync-capabilities.js'
import { fetchCapabilityReport } from './sync-capabilities.js'
import { RemoteError } from './types.js'

export const SQL_REFUSED_MESSAGE =
  'This server does not accept SQL over the network. Call a registered operation by name, or start the server with acceptSql: true.'

export interface ServerCapabilityCheck {
  assertSqlAccepted(): Promise<void>
  registryDigest(refresh?: boolean): Promise<string | undefined>
}

export class ServerCapabilities implements ServerCapabilityCheck {
  private pending: Promise<CapabilityReport> | null = null

  constructor(
    private readonly resolveUrl: () => string | Promise<string>,
    private readonly headers: Record<string, string> | undefined,
    private readonly requestTimeoutMs: number = DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
  ) {}

  async read(refresh = false): Promise<CapabilityReport> {
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
    return (await this.read(refresh)).registryDigest
  }

  async assertSqlAccepted(): Promise<void> {
    const report = await this.read()
    if (!report.capabilities.includes(SQL_QUERY_CAPABILITY)) {
      throw new RemoteError('SQL_NOT_ACCEPTED', SQL_REFUSED_MESSAGE)
    }
  }

  private async fetch(): Promise<CapabilityReport> {
    const url = await this.resolveUrl()
    try {
      return await fetchCapabilityReport({ url, headers: this.headers, requestTimeoutMs: this.requestTimeoutMs })
    } catch (err) {
      if (err instanceof RemoteError && err.code === 'NOT_FOUND') {
        return { capabilities: [SQL_QUERY_CAPABILITY], registryDigest: undefined }
      }
      throw err
    }
  }
}
