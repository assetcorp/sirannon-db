import type uWS from 'uWebSockets.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import type { ResolvedCors } from './cors.js'
import { writeCorsOrigin } from './cors.js'
import {
  DATABASE_ID_SEGMENT,
  decodePathSegment,
  initAbortHandler,
  OPERATION_NAME_SEGMENT,
  readBody,
  sendError,
  sendUndecodablePathSegment,
} from './http-common.js'
import type { OperationRouteHandler } from './http-operations.js'
import { decodeRemoteAddress, runAuthenticate } from './request-hook.js'

export interface OperationRouteDeps<Identity> {
  cors: ResolvedCors | null
  maxBodyBytes: number
  authenticateHook: AuthenticateHook<Identity> | undefined
}

function sendUndecodableSegment(res: uWS.HttpResponse, rawId: string, dbId: string | null, rawName: string): void {
  if (dbId === null) {
    sendUndecodablePathSegment(res, DATABASE_ID_SEGMENT, rawId)
    return
  }
  sendUndecodablePathSegment(res, OPERATION_NAME_SEGMENT, rawName)
}

export function wrapOperationRoute<Identity>(
  deps: OperationRouteDeps<Identity>,
  handler: OperationRouteHandler,
): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
  return (res, req) => {
    const rawId = req.getParameter(0) ?? ''
    const rawName = req.getParameter(1) ?? ''
    const dbId = decodePathSegment(rawId)
    const name = decodePathSegment(rawName)
    const method = req.getMethod()
    const path = req.getUrl()

    if (deps.cors) writeCorsOrigin(res, deps.cors, req.getHeader('origin'))

    if (!deps.authenticateHook && (dbId === null || name === null)) {
      sendUndecodableSegment(res, rawId, dbId, rawName)
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
      databaseId: dbId ?? rawId,
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
      if (dbId === null || name === null) {
        sendUndecodableSegment(res, rawId, dbId, rawName)
        return
      }
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
