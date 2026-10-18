export interface ToxiproxyLink {
  name: string
  listenPort: number
  upstreamPort: number
}

export interface ToxiproxyHosts {
  apiBaseUrl: string
  listenHost: string
  upstreamHost: string
}

export class ToxiproxyClient {
  private readonly apiBaseUrl: string

  constructor(private readonly hosts: ToxiproxyHosts) {
    this.apiBaseUrl = hosts.apiBaseUrl
  }

  async createProxy(link: ToxiproxyLink): Promise<void> {
    await this.request('/proxies', {
      method: 'POST',
      body: {
        name: link.name,
        listen: `${this.hosts.listenHost}:${link.listenPort}`,
        upstream: `${this.hosts.upstreamHost}:${link.upstreamPort}`,
        enabled: true,
      },
    })
  }

  async setEnabled(proxyName: string, enabled: boolean): Promise<void> {
    await this.request(`/proxies/${encodeURIComponent(proxyName)}`, {
      method: 'POST',
      body: { enabled },
    })
  }

  async addLatency(proxyName: string, name: string, latencyMs: number): Promise<void> {
    for (const stream of ['downstream', 'upstream'] as const) {
      await this.request(`/proxies/${encodeURIComponent(proxyName)}/toxics`, {
        method: 'POST',
        body: {
          name: `${name}-${stream}`,
          type: 'latency',
          stream,
          toxicity: 1,
          attributes: {
            latency: latencyMs,
            jitter: Math.max(1, Math.floor(latencyMs / 5)),
          },
        },
      })
    }
  }

  async clearToxic(proxyName: string, toxicName: string): Promise<void> {
    for (const stream of ['downstream', 'upstream'] as const) {
      await this.request(
        `/proxies/${encodeURIComponent(proxyName)}/toxics/${encodeURIComponent(`${toxicName}-${stream}`)}`,
        { method: 'DELETE' },
      )
    }
  }

  async reset(): Promise<void> {
    await this.request('/reset', { method: 'POST' })
  }

  private async request(path: string, options: { method: string; body?: Record<string, unknown> }): Promise<unknown> {
    const response = await fetch(`${this.apiBaseUrl}${path}`, {
      method: options.method,
      headers: options.body ? { 'content-type': 'application/json' } : undefined,
      body: options.body ? JSON.stringify(options.body) : undefined,
    })
    if (response.ok) {
      const text = await response.text()
      return text.trim() === '' ? null : JSON.parse(text)
    }
    const body = await response.text().catch(() => '')
    throw new Error(`Toxiproxy ${options.method} ${path} failed with ${response.status}: ${body}`)
  }
}
