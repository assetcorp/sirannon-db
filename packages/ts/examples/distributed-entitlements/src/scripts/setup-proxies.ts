interface ProxySpec {
  name: string
  listen: string
  upstream: string
}

export {}

const NODE_IDS = ['node-a', 'node-b', 'node-c'] as const
const TOXIPROXY_URL = process.env.TOXIPROXY_URL ?? 'http://127.0.0.1:8474'
const TOXIPROXY_READY_TIMEOUT_MS = 30_000
const TOXIPROXY_REQUEST_TIMEOUT_MS = 5_000

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
  const deadline = Date.now() + TOXIPROXY_READY_TIMEOUT_MS
  while (Date.now() < deadline) {
    try {
      const remainingMs = deadline - Date.now()
      const response = await fetchWithTimeout(`${TOXIPROXY_URL}/proxies`, undefined, Math.min(remainingMs, 1_000))
      if (response.ok) return
    } catch {
      const remainingMs = deadline - Date.now()
      if (remainingMs > 0) {
        await delay(Math.min(remainingMs, 500))
      }
      continue
    }
    const remainingMs = deadline - Date.now()
    if (remainingMs > 0) {
      await delay(Math.min(remainingMs, 500))
    }
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
  let response: Response
  try {
    response = await fetchWithTimeout(
      `${TOXIPROXY_URL}${path}`,
      {
        method: options.method,
        headers: options.body ? { 'content-type': 'application/json' } : undefined,
        body: options.body ? JSON.stringify(options.body) : undefined,
      },
      TOXIPROXY_REQUEST_TIMEOUT_MS,
    )
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Toxiproxy ${options.method} ${path} timed out after ${TOXIPROXY_REQUEST_TIMEOUT_MS}ms`)
    }
    throw error
  }

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

async function fetchWithTimeout(url: string, init: RequestInit | undefined, timeoutMs: number): Promise<Response> {
  if (timeoutMs <= 0) {
    throw new Error(`Toxiproxy request timed out before contacting ${url}`)
  }

  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort()
  }, timeoutMs)

  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timeout)
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms)
  })
}
