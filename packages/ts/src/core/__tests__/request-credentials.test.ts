import { describe, expect, it } from 'vitest'
import {
  readBearerToken,
  readHeader,
  readSubprotocolCredential,
  toSubprotocolCredential,
} from '../request-credentials.js'
import type { RequestContext } from '../server-options.js'

const AUTH_PREFIX = 'sirannon.demo.auth.'

function context(headers: Record<string, string>, overrides: Partial<RequestContext> = {}): RequestContext {
  return {
    headers,
    method: 'POST',
    path: '/db/main/changes',
    databaseId: 'main',
    remoteAddress: '127.0.0.1',
    ...overrides,
  }
}

describe('toSubprotocolCredential', () => {
  it('encodes a token as a single header token a handshake can offer', () => {
    const offered = toSubprotocolCredential(AUTH_PREFIX, 'sirannon-demo-token')

    expect(offered.startsWith(AUTH_PREFIX)).toBe(true)
    expect(offered).toMatch(/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/)
  })

  it('round-trips a token carrying characters a subprotocol refuses', () => {
    const token = 'ticket for device 7, issued at 09:00+01:00'
    const ctx = context({ 'sec-websocket-protocol': `sirannon.v1, ${toSubprotocolCredential(AUTH_PREFIX, token)}` })

    expect(readSubprotocolCredential(ctx, AUTH_PREFIX)).toBe(token)
  })

  it('round-trips a token carrying characters outside the Latin-1 range', () => {
    const token = 'jeton-café-🔑'
    const ctx = context({ 'sec-websocket-protocol': toSubprotocolCredential(AUTH_PREFIX, token) })

    expect(readSubprotocolCredential(ctx, AUTH_PREFIX)).toBe(token)
  })
})

describe('readSubprotocolCredential', () => {
  it('returns undefined when the offer carries no entry under the prefix', () => {
    const ctx = context({ 'sec-websocket-protocol': 'sirannon.v1' })

    expect(readSubprotocolCredential(ctx, AUTH_PREFIX)).toBeUndefined()
  })

  it('returns undefined when the header is absent', () => {
    expect(readSubprotocolCredential(context({}), AUTH_PREFIX)).toBeUndefined()
  })

  it('returns undefined for an entry whose payload is not valid base64url', () => {
    const ctx = context({ 'sec-websocket-protocol': `${AUTH_PREFIX}!!!!` })

    expect(readSubprotocolCredential(ctx, AUTH_PREFIX)).toBeUndefined()
  })
})

describe('readBearerToken', () => {
  it('reads the token from an Authorization header of any casing', () => {
    expect(readBearerToken(context({ Authorization: 'Bearer abc123' }))).toBe('abc123')
    expect(readBearerToken(context({ authorization: 'Bearer abc123' }))).toBe('abc123')
    expect(readBearerToken(context({ AUTHORIZATION: 'Bearer abc123' }))).toBe('abc123')
  })

  it('returns undefined for a scheme other than Bearer', () => {
    expect(readBearerToken(context({ authorization: 'Basic abc123' }))).toBeUndefined()
  })

  it('returns undefined for a Bearer header carrying no token', () => {
    expect(readBearerToken(context({ authorization: 'Bearer ' }))).toBeUndefined()
  })
})

describe('readHeader', () => {
  it('finds a header whatever casing the runtime reports', () => {
    expect(readHeader(context({ 'Sec-WebSocket-Protocol': 'sirannon.v1' }), 'sec-websocket-protocol')).toBe(
      'sirannon.v1',
    )
  })

  it('returns undefined when no header matches', () => {
    expect(readHeader(context({ origin: 'http://localhost' }), 'authorization')).toBeUndefined()
  })
})
