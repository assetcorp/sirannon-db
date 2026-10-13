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

const HEADER_NAME_TOKEN = SUBPROTOCOL_TOKEN

const NUL_CODE = 0x00

const LINE_FEED_CODE = 0x0a

const CARRIAGE_RETURN_CODE = 0x0d

const HIGHEST_HEADER_VALUE_CODE = 0xff

const MALFORMED_HEADER_NAME_REASON = 'a header name is one or more of the characters a header token allows'

const MALFORMED_HEADER_VALUE_REASON =
  'a header value carries no line break, no NUL character, and no character above U+00FF'

function subprotocolMessage(index: number, reason: string): string {
  return `Entry ${index} of 'webSocketProtocols' cannot be offered, because ${reason}. The entry carries a credential, so it is left out of this message.`
}

function isSendableHeaderValue(value: string): boolean {
  for (let position = 0; position < value.length; position++) {
    const code = value.charCodeAt(position)
    if (code === NUL_CODE || code === LINE_FEED_CODE || code === CARRIAGE_RETURN_CODE) return false
    if (code > HIGHEST_HEADER_VALUE_CODE) return false
  }
  return true
}

function headerMessage(index: number, reason: string): string {
  return `Entry ${index} of 'headers' cannot be sent, because ${reason}. The entry can carry a credential, so it is left out of this message.`
}

/**
 * Throws a `RemoteError` with code `INVALID_ARGUMENT` when a configured header cannot travel in an HTTP request or a
 * WebSocket upgrade, so that the client refuses it once at construction and never copies a credential into the error
 * of a failed request.
 *
 * @param headers - The headers that the caller configured, or undefined when it configured none.
 *
 * @internal
 */
export function assertHeadersSendable(headers: Record<string, string> | undefined): void {
  if (headers === undefined) return
  for (const [index, [name, value]] of Object.entries(headers).entries()) {
    if (!HEADER_NAME_TOKEN.test(name)) {
      throw new RemoteError('INVALID_ARGUMENT', headerMessage(index, MALFORMED_HEADER_NAME_REASON))
    }
    if (!isSendableHeaderValue(String(value))) {
      throw new RemoteError('INVALID_ARGUMENT', headerMessage(index, MALFORMED_HEADER_VALUE_REASON))
    }
  }
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
