export const DATABASE_ID = 'main'
export const DEFAULT_DATA_ENDPOINT = 'http://localhost:9876'
export const DEFAULT_DEMO_TOKEN = 'sirannon-demo-token'
export const WAREHOUSE_DEMO_TOKEN = 'sirannon-warehouse-token'
export const WEBSOCKET_AUTH_PROTOCOL_PREFIX = 'sirannon.demo.auth.'

export function toWebSocketAuthProtocol(token: string): string {
  const bytes = new TextEncoder().encode(token)
  let binary = ''

  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }

  const encoded = btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
  return `${WEBSOCKET_AUTH_PROTOCOL_PREFIX}${encoded}`
}
