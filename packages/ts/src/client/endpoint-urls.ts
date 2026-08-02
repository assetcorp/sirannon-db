export function normaliseEndpointUrl(rawUrl: string): string {
  let parsed: URL
  try {
    parsed = new URL(rawUrl)
  } catch {
    throw new TypeError(`Endpoint URL '${rawUrl}' is invalid`)
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new TypeError(`Endpoint URL '${rawUrl}' must use http or https`)
  }
  if (parsed.username || parsed.password) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain credentials`)
  }
  if (parsed.hash) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain a fragment`)
  }
  if (parsed.search) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain a query string`)
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, '')
  return parsed.toString().replace(/\/$/, '')
}

export function toBaseUrl(url: string): string {
  return normaliseEndpointUrl(url)
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

export function toServerBaseUrl(url: string, databaseId?: string): string {
  const base = toBaseUrl(url)
  if (!databaseId) return base.replace(/\/db\/[^/]+$/i, '')
  return base.replace(new RegExp(`/db/${escapeRegExp(encodeURIComponent(databaseId))}$`, 'i'), '')
}

export function toWsUrl(baseUrl: string): string {
  return baseUrl.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://')
}
