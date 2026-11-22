export const LOOPBACK_HOST = '127.0.0.1'

export function loopbackEndpoint(port: number): string {
  return `${LOOPBACK_HOST}:${port}`
}
