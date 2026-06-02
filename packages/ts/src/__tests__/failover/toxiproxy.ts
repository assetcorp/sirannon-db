export interface ToxiproxyLink {
  name: string
  listenPort: number
  upstreamPort: number
}

export class ToxiproxyClient {
  constructor(private readonly apiBaseUrl: string) {}

  async createProxy(link: ToxiproxyLink): Promise<void> {
    await this.request('/proxies', {
      method: 'POST',
      body: {
        name: link.name,
        listen: `0.0.0.0:${link.listenPort}`,
        upstream: `host.docker.internal:${link.upstreamPort}`,
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
    await this.request(`/proxies/${encodeURIComponent(proxyName)}/toxics`, {
      method: 'POST',
      body: {
        name,
        type: 'latency',
        stream: 'downstream',
        toxicity: 1,
        attributes: {
          latency: latencyMs,
          jitter: Math.max(1, Math.floor(latencyMs / 5)),
        },
      },
    })
  }

  async clearToxic(proxyName: string, toxicName: string): Promise<void> {
    await this.request(`/proxies/${encodeURIComponent(proxyName)}/toxics/${encodeURIComponent(toxicName)}`, {
      method: 'DELETE',
    })
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
