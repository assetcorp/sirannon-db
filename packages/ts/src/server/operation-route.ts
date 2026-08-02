import type uWS from 'uWebSockets.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import type { ResolvedCors } from './cors.js'
import { writeCorsOrigin } from './cors.js'
import { initAbortHandler, readBody, sendError } from './http-common.js'
import type { OperationRouteHandler } from './http-operations.js'
import { decodeRemoteAddress, runAuthenticate } from './request-hook.js'

export interface OperationRouteDeps<Identity> {
  cors: ResolvedCors | null
  maxBodyBytes: number
  authenticateHook: AuthenticateHook<Identity> | undefined
}

function decodePathSegment(value: string): string | null {
  try {
    return decodeURIComponent(value)
  } catch {
    return null
  }
}

export function wrapOperationRoute<Identity>(
  deps: OperationRouteDeps<Identity>,
  handler: OperationRouteHandler,
): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
  return (res, req) => {
    const dbId = req.getParameter(0) ?? ''
    const rawName = req.getParameter(1) ?? ''
    const method = req.getMethod()
    const path = req.getUrl()

    if (deps.cors) writeCorsOrigin(res, deps.cors, req.getHeader('origin'))

    const name = decodePathSegment(rawName)
    if (name === null) {
      sendError(res, 400, 'INVALID_REQUEST', `Operation name '${rawName}' is not valid percent-encoding`)
      return
    }

    const headers: Record<string, string> = {}
    req.forEach((key, value) => {
      headers[key] = value
    })

    const abort = initAbortHandler(res)
    const bodyPromise = readBody(res, deps.maxBodyBytes, abort)

    const ctx: RequestContext = {
      headers,
      method,
      path,
      databaseId: dbId,
      remoteAddress: decodeRemoteAddress(res),
    }

    const run = async (): Promise<void> => {
      const rawBody = await bodyPromise

      let identity: Identity | undefined
      if (deps.authenticateHook) {
        const authenticated = await runAuthenticate(res, abort, ctx, deps.authenticateHook)
        if (!authenticated.ok) return
        identity = authenticated.identity
      }

      if (!abort.claim()) return
      try {
        await handler(res, dbId, name, identity, rawBody, abort)
      } catch {
        if (!abort.aborted) {
          sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
        }
      }
    }

    run().catch(() => {})
  }
}
