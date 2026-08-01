import { RemoteError } from '../types.js'

export interface HandshakeRuntime {
  process?: { versions?: { node?: string } }
  Deno?: unknown
  window?: unknown
}

const HEADERS_UNSUPPORTED_MESSAGE =
  "This runtime builds a WebSocket from the global constructor, which carries no handshake header, so 'headers' reaches the server on HTTP requests but never on the WebSocket upgrade. Carry the credential in 'webSocketProtocols' as well, which a browser handshake does carry, or create the client with { transport: 'http' }."

function currentRuntime(): HandshakeRuntime {
  return globalThis as unknown as HandshakeRuntime
}

export function runtimeSupportsHandshakeHeaders(runtime: HandshakeRuntime = currentRuntime()): boolean {
  if (runtime.Deno !== undefined || runtime.window !== undefined) return false
  return typeof runtime.process?.versions?.node === 'string'
}

function carriesSubprotocolCredential(protocols: string | string[] | undefined): boolean {
  if (protocols === undefined) return false
  return typeof protocols === 'string' ? protocols.length > 0 : protocols.length > 0
}

export function assertHandshakeHeadersSupported(
  headers: Record<string, string> | undefined,
  webSocketProtocols: string | string[] | undefined,
  runtime: HandshakeRuntime = currentRuntime(),
): void {
  if (headers === undefined || Object.keys(headers).length === 0) return
  if (carriesSubprotocolCredential(webSocketProtocols)) return
  if (runtimeSupportsHandshakeHeaders(runtime)) return
  throw new RemoteError('INVALID_ARGUMENT', HEADERS_UNSUPPORTED_MESSAGE)
}
