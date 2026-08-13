import { RemoteError } from '../types.js'

export interface HandshakeRuntime {
  process?: { versions?: { node?: string } }
  Deno?: unknown
  window?: unknown
}

const HEADERS_UNSUPPORTED_MESSAGE =
  "This runtime builds a WebSocket from the global constructor, which carries no handshake header, so 'headers' reaches the server on HTTP requests but never on the WebSocket upgrade. Carry the credential in 'webSocketProtocols' as well, which a browser handshake does carry, or create the client with { transport: 'http' }."

const SUBPROTOCOL_TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/

const MALFORMED_SUBPROTOCOL_REASON =
  'a subprotocol is one or more of the characters a header token allows, so it carries no space, comma, or quotation mark and is never empty'

const REPEATED_SUBPROTOCOL_REASON = 'a handshake refuses an offer that repeats a subprotocol'

function subprotocolMessage(index: number, reason: string): string {
  return `Entry ${index} of 'webSocketProtocols' cannot be offered, because ${reason}. The entry carries a credential, so it is left out of this message.`
}

function currentRuntime(): HandshakeRuntime {
  return globalThis as unknown as HandshakeRuntime
}

export function runtimeSupportsHandshakeHeaders(runtime: HandshakeRuntime = currentRuntime()): boolean {
  if (runtime.Deno !== undefined || runtime.window !== undefined) return false
  return typeof runtime.process?.versions?.node === 'string'
}

function carriesSubprotocolCredential(protocols: string | string[] | undefined): boolean {
  return protocols !== undefined && protocols.length > 0
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

export function assertWebSocketProtocolsValid(webSocketProtocols: string | string[] | undefined): void {
  if (webSocketProtocols === undefined) return
  const offered = typeof webSocketProtocols === 'string' ? [webSocketProtocols] : webSocketProtocols
  const seen = new Set<string>()
  for (const [index, value] of offered.entries()) {
    if (!SUBPROTOCOL_TOKEN.test(value)) {
      throw new RemoteError('INVALID_ARGUMENT', subprotocolMessage(index, MALFORMED_SUBPROTOCOL_REASON))
    }
    if (seen.has(value)) {
      throw new RemoteError('INVALID_ARGUMENT', subprotocolMessage(index, REPEATED_SUBPROTOCOL_REASON))
    }
    seen.add(value)
  }
}

export function assertWebSocketCredentials(
  headers: Record<string, string> | undefined,
  webSocketProtocols: string | string[] | undefined,
  runtime: HandshakeRuntime = currentRuntime(),
): void {
  assertWebSocketProtocolsValid(webSocketProtocols)
  assertHandshakeHeadersSupported(headers, webSocketProtocols, runtime)
}
