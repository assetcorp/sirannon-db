interface ProxySpec {
  name: string
  listen: string
  upstream: string
}

export {}

const NODE_IDS = ['node-a', 'node-b', 'node-c'] as const
const TOXIPROXY_URL = process.env.TOXIPROXY_URL ?? 'http://127.0.0.1:8474'

const proxies: ProxySpec[] = [
  ...NODE_IDS.map((nodeId, index) => ({
    name: `etcd-entitlements-${nodeId}`,
    listen: `0.0.0.0:${4101 + index}`,
    upstream: 'etcd:2379',
  })),
  ...NODE_IDS.map((nodeId, index) => ({
    name: `grpc-entitlements-${nodeId}`,
    listen: `0.0.0.0:${5101 + index}`,
    upstream: `${nodeId}:${7401 + index}`,
  })),
]

await waitForToxiproxy()

for (const proxy of proxies) {
  await deleteProxy(proxy.name)
  await createProxy(proxy)
}

console.log(`Created ${proxies.length} Toxiproxy links`)

async function waitForToxiproxy(): Promise<void> {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${TOXIPROXY_URL}/proxies`)
      if (response.ok) return
    } catch {
      await delay(500)
      continue
    }
    await delay(500)
  }
  throw new Error(`Toxiproxy did not become ready at ${TOXIPROXY_URL}`)
}

async function createProxy(proxy: ProxySpec): Promise<void> {
  await request('/proxies', {
    method: 'POST',
    body: {
      name: proxy.name,
      listen: proxy.listen,
      upstream: proxy.upstream,
      enabled: true,
    },
  })
}

async function deleteProxy(name: string): Promise<void> {
  await request(`/proxies/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    tolerateNotFound: true,
  })
}

async function request(
  path: string,
  options: { method: string; body?: Record<string, unknown>; tolerateNotFound?: boolean },
): Promise<unknown> {
  const response = await fetch(`${TOXIPROXY_URL}${path}`, {
    method: options.method,
    headers: options.body ? { 'content-type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
  })

  if (response.ok) {
    const text = await response.text()
    return text.trim() === '' ? null : JSON.parse(text)
  }

  if (options.tolerateNotFound && response.status === 404) {
    return null
  }

  const body = await response.text().catch(() => '')
  throw new Error(`Toxiproxy ${options.method} ${path} failed with ${response.status}: ${body}`)
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms)
  })
}
