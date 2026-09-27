import {
  type RequestContext,
  RequestDeniedError,
  readBearerToken,
  readHeader,
  readSubprotocolCredential,
} from '@delali/sirannon-db'
import { DEFAULT_DEMO_TOKEN, WAREHOUSE_DEMO_TOKEN, WEBSOCKET_AUTH_PROTOCOL_PREFIX } from './lib/demo-config'
import type { Operator } from './operations'

const OPERATORS_BY_TOKEN = new Map<string, string>([
  [process.env.SIRANNON_DEMO_TOKEN ?? DEFAULT_DEMO_TOKEN, 'ops-console'],
  [WAREHOUSE_DEMO_TOKEN, 'warehouse-floor'],
])

export function createOperatorAuthenticator(
  allowedOrigins: readonly string[],
  databaseId: string,
): (ctx: RequestContext) => Operator {
  const upgradePath = `/db/${databaseId}`

  return ctx => {
    if (ctx.method.toUpperCase() === 'GET' && ctx.path === upgradePath) {
      const origin = readHeader(ctx, 'origin')
      if (origin === undefined || !allowedOrigins.includes(origin)) {
        throw new RequestDeniedError(
          403,
          'FORBIDDEN_ORIGIN',
          'A WebSocket upgrade to the demo data server must include an Origin header that matches an application origin.',
        )
      }

      const ticket = readSubprotocolCredential(ctx, WEBSOCKET_AUTH_PROTOCOL_PREFIX)
      const operatorId = ticket === undefined ? undefined : OPERATORS_BY_TOKEN.get(ticket)
      if (operatorId === undefined) {
        throw new RequestDeniedError(
          401,
          'UNAUTHORIZED',
          'A WebSocket upgrade to the demo data server must include a subprotocol credential that matches a known operator.',
        )
      }

      return { operatorId }
    }

    const token = readBearerToken(ctx)
    const operatorId = token === undefined ? undefined : OPERATORS_BY_TOKEN.get(token)
    if (operatorId === undefined) {
      throw new RequestDeniedError(
        401,
        'UNAUTHORIZED',
        'A request to the demo data server must include a bearer token that matches a known operator.',
      )
    }

    return { operatorId }
  }
}
