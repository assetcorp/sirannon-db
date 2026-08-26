import {
  type RequestContext,
  RequestDeniedError,
  readBearerToken,
  readHeader,
  readSubprotocolCredential,
} from '@delali/sirannon-db'
import { DEFAULT_DEVICE_TOKEN, DEVICE_AUTH_PROTOCOL_PREFIX } from './lib/demo-config'

export interface FieldTechnician {
  fleetId: string
}

const FLEET_BY_TOKEN = new Map<string, string>([
  [process.env.SIRANNON_DEVICE_TOKEN ?? DEFAULT_DEVICE_TOKEN, 'field-service'],
])

export function createDeviceAuthenticator(
  allowedOrigins: readonly string[],
  databaseId: string,
): (ctx: RequestContext) => FieldTechnician {
  const upgradePath = `/db/${databaseId}`

  return ctx => {
    const origin = readHeader(ctx, 'origin')
    if (origin !== undefined && !allowedOrigins.includes(origin)) {
      throw new RequestDeniedError(
        403,
        'FORBIDDEN_ORIGIN',
        'The field service data server accepts requests from its own application origins only.',
      )
    }

    const isUpgrade = ctx.method.toUpperCase() === 'GET' && ctx.path === upgradePath
    const token = isUpgrade ? readSubprotocolCredential(ctx, DEVICE_AUTH_PROTOCOL_PREFIX) : readBearerToken(ctx)
    const fleetId = token === undefined ? undefined : FLEET_BY_TOKEN.get(token)

    if (fleetId === undefined) {
      throw new RequestDeniedError(
        401,
        'UNAUTHORIZED',
        'A device request must carry a token naming a known fleet, as a bearer header or a WebSocket subprotocol.',
      )
    }

    return { fleetId }
  }
}
