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
