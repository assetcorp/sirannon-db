export const DATABASE_ID = 'entitlements'
export const DEFAULT_CLUSTER_ENDPOINTS = ['http://127.0.0.1:7301', 'http://127.0.0.1:7302', 'http://127.0.0.1:7303']
export const DEFAULT_CLUSTER_TOKEN = 'sirannon-entitlements-local-token'
export const WEBSOCKET_AUTH_PROTOCOL_PREFIX = 'sirannon.entitlements.auth.'

export function clusterEndpointsFromEnv(value: string | undefined): string[] {
  if (!value) {
    return [...DEFAULT_CLUSTER_ENDPOINTS]
  }

  const endpoints = value
    .split(',')
    .map(endpoint => endpoint.trim())
    .filter(endpoint => endpoint.length > 0)

  return endpoints.length > 0 ? endpoints : [...DEFAULT_CLUSTER_ENDPOINTS]
}

export function toServerBaseUrl(endpoint: string): string {
  return endpoint
    .replace(/\/+$/, '')
    .replace(new RegExp(`/db/${DATABASE_ID}$`, 'i'), '')
    .replace(/\/+$/, '')
}

export function toWebSocketAuthProtocol(token: string): string {
  return `${WEBSOCKET_AUTH_PROTOCOL_PREFIX}${toBase64Url(token)}`
}

function toBase64Url(value: string): string {
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(value, 'utf8').toString('base64url')
  }

  const bytes = new TextEncoder().encode(value)
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}
