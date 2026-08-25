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
          'The demo data server rejects WebSocket upgrades from untrusted origins.',
        )
      }

      const ticket = readSubprotocolCredential(ctx, WEBSOCKET_AUTH_PROTOCOL_PREFIX)
      const operatorId = ticket === undefined ? undefined : OPERATORS_BY_TOKEN.get(ticket)
      if (operatorId === undefined) {
        throw new RequestDeniedError(
          401,
          'UNAUTHORIZED',
          'The demo data server requires a WebSocket auth protocol naming a known operator.',
        )
      }

      return { operatorId }
    }

    const token = readBearerToken(ctx)
    const operatorId = token === undefined ? undefined : OPERATORS_BY_TOKEN.get(token)
    if (operatorId === undefined) {
      throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The demo data server requires a bearer token for an operator.')
    }

    return { operatorId }
  }
}
