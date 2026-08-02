import { type RequestContext, RequestDeniedError } from '@delali/sirannon-db'
import { DEFAULT_DEMO_TOKEN, toWebSocketAuthProtocol, WAREHOUSE_DEMO_TOKEN } from './lib/demo-config'
import type { Operator } from './operations'

const OPERATORS_BY_TOKEN = new Map<string, string>([
  [process.env.SIRANNON_DEMO_TOKEN ?? DEFAULT_DEMO_TOKEN, 'ops-console'],
  [WAREHOUSE_DEMO_TOKEN, 'warehouse-floor'],
])

const OPERATORS_BY_PROTOCOL = new Map(
  [...OPERATORS_BY_TOKEN].map(([token, operatorId]) => [toWebSocketAuthProtocol(token), operatorId]),
)

export function getHeader(headers: Record<string, string>, name: string): string | undefined {
  const direct = headers[name] ?? headers[name.toLowerCase()]
  if (direct !== undefined) {
    return direct
  }

  const lowerName = name.toLowerCase()
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === lowerName) {
      return value
    }
  }

  return undefined
}

function operatorForBearer(value: string | undefined): string | undefined {
  if (value === undefined || !value.startsWith('Bearer ')) {
    return undefined
  }

  return OPERATORS_BY_TOKEN.get(value.slice('Bearer '.length))
}

function operatorForWebSocketProtocol(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined
  }

  for (const protocol of value.split(',')) {
    const operatorId = OPERATORS_BY_PROTOCOL.get(protocol.trim())
    if (operatorId !== undefined) {
      return operatorId
    }
  }

  return undefined
}

export function createOperatorAuthenticator(
  allowedOrigins: readonly string[],
  databaseId: string,
): (ctx: RequestContext) => Operator {
  const upgradePath = `/db/${databaseId}`

  return ctx => {
    if (ctx.method.toUpperCase() === 'GET' && ctx.path === upgradePath) {
      const origin = getHeader(ctx.headers, 'origin')
      if (origin === undefined || !allowedOrigins.includes(origin)) {
        throw new RequestDeniedError(
          403,
          'FORBIDDEN_ORIGIN',
          'The demo data server rejects WebSocket upgrades from untrusted origins.',
        )
      }

      const operatorId = operatorForWebSocketProtocol(getHeader(ctx.headers, 'sec-websocket-protocol'))
      if (operatorId === undefined) {
        throw new RequestDeniedError(
          401,
          'UNAUTHORIZED',
          'The demo data server requires a WebSocket auth protocol naming a known operator.',
        )
      }

      return { operatorId }
    }

    const operatorId = operatorForBearer(getHeader(ctx.headers, 'authorization'))
    if (operatorId === undefined) {
      throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The demo data server requires a bearer token for an operator.')
    }

    return { operatorId }
  }
}
