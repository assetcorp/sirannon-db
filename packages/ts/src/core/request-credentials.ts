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
 * Packs a credential into one WebSocket subprotocol a handshake can offer.
 *
 * A browser sends no header on a WebSocket upgrade, so a credential travels as a
 * subprotocol instead, and a subprotocol accepts only the characters a header
 * token allows. This encodes the credential so that any text survives the trip,
 * and {@link readSubprotocolCredential} reads it back on the server.
 *
 * @param prefix - Text naming the scheme, which the server matches on.
 * @param credential - The credential to carry.
 * @returns The subprotocol to pass as `webSocketProtocols`.
 *
 * @public
 */
export function toSubprotocolCredential(prefix: string, credential: string): string {
  return `${prefix}${toBase64Url(new TextEncoder().encode(credential))}`
}

/**
 * Reads a header from a request, whatever casing the runtime reported it under.
 *
 * @param ctx - The request the `authenticate` hook received.
 * @param name - Name of the header to read.
 * @returns The header value, or undefined where the request carries none.
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
 * Reads the token from a request's `Authorization: Bearer` header.
 *
 * @param ctx - The request the `authenticate` hook received.
 * @returns The token, or undefined where the request carries another scheme or none.
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
 * Reads the credential a WebSocket upgrade offered under a prefix.
 *
 * The upgrade carries the whole offer, including the `sirannon.v1` identifier the
 * client sends ahead of its own entries, so this picks the first entry under the
 * prefix and unpacks what {@link toSubprotocolCredential} put there.
 *
 * @param ctx - The request the `authenticate` hook received.
 * @param prefix - The same text passed to {@link toSubprotocolCredential}.
 * @returns The credential, or undefined where the offer carries none under that prefix.
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
