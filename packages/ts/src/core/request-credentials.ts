import type { RequestContext } from './server-options.js'

const BEARER_SCHEME = 'Bearer '

function toBase64Url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function fromBase64Url(value: string): string | undefined {
  const standard = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = standard + '='.repeat((4 - (standard.length % 4)) % 4)
  try {
    const binary = atob(padded)
    const bytes = new Uint8Array(binary.length)
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index)
    }
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes)
  } catch {
    return undefined
  }
}

/**
 * Encodes a credential as one WebSocket subprotocol that a handshake can offer.
 *
 * A browser sends no custom header on a WebSocket upgrade, so the client sends
 * the credential as a subprotocol, which may contain only the characters that a
 * header token allows. This function encodes the credential in base64url so
 * that any text fits, and {@link readSubprotocolCredential} decodes it on the
 * server.
 *
 * @param prefix - The text that names the scheme, which the server matches on.
 * @param credential - The credential to encode.
 * @returns The subprotocol to pass as `webSocketProtocols`.
 *
 * @public
 */
export function toSubprotocolCredential(prefix: string, credential: string): string {
  return `${prefix}${toBase64Url(new TextEncoder().encode(credential))}`
}

/**
 * Returns a header from a request, whatever the casing of the header name that the runtime reported.
 *
 * @param ctx - The request that Sirannon passed to the `authenticate` hook.
 * @param name - The name of the header to return.
 * @returns The header value, or undefined when the request has no such header.
 *
 * @public
 */
export function readHeader(ctx: RequestContext, name: string): string | undefined {
  const lowerName = name.toLowerCase()
  const direct = ctx.headers[name] ?? ctx.headers[lowerName]
  if (direct !== undefined) return direct

  for (const [key, value] of Object.entries(ctx.headers)) {
    if (key.toLowerCase() === lowerName) return value
  }

  return undefined
}

/**
 * Returns the token from a request's `Authorization: Bearer` header.
 *
 * @param ctx - The request that Sirannon passed to the `authenticate` hook.
 * @returns The token, or undefined when the request has no `Authorization` header, uses another scheme, or sends an empty token.
 *
 * @public
 */
export function readBearerToken(ctx: RequestContext): string | undefined {
  const value = readHeader(ctx, 'authorization')
  if (value === undefined || !value.startsWith(BEARER_SCHEME)) return undefined

  const token = value.slice(BEARER_SCHEME.length)
  return token.length > 0 ? token : undefined
}

/**
 * Returns the credential that a WebSocket upgrade offered under a prefix.
 *
 * The upgrade request lists every offered subprotocol, including the
 * `sirannon.v1` identifier that the client sends ahead of its own entries, so
 * this function takes the first entry that starts with the prefix and decodes
 * what {@link toSubprotocolCredential} encoded there.
 *
 * @param ctx - The request that Sirannon passed to the `authenticate` hook.
 * @param prefix - The same text that you passed to {@link toSubprotocolCredential}.
 * @returns The credential, or undefined when no offered subprotocol starts with that prefix or its value fails to decode.
 *
 * @public
 */
export function readSubprotocolCredential(ctx: RequestContext, prefix: string): string | undefined {
  const offer = readHeader(ctx, 'sec-websocket-protocol')
  if (offer === undefined) return undefined

  for (const entry of offer.split(',')) {
    const trimmed = entry.trim()
    if (trimmed.startsWith(prefix)) {
      return fromBase64Url(trimmed.slice(prefix.length))
    }
  }

  return undefined
}
