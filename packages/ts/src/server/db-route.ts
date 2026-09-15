import type { HttpRequest, HttpResponse } from 'uWebSockets.js'
import type { RequestContext } from '../core/types.js'
import { writeCorsOrigin } from './cors.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'
import { initAbortHandler, readBody, sendError } from './http-handler.js'
import type { OperationRouteDeps } from './operation-route.js'
import { decodeRemoteAddress, runAuthenticate } from './request-hook.js'

function readHeaders(req: HttpRequest): Record<string, string> {
  const headers: Record<string, string> = {}
  req.forEach((key, value) => {
    headers[key] = value
  })
  return headers
}

export function wrapDbRoute<Identity>(
  deps: OperationRouteDeps<Identity>,
  handler: DbRouteHandler,
): (res: HttpResponse, req: HttpRequest) => void {
  const { authenticateHook, cors, maxBodyBytes } = deps

  return (res, req) => {
    const dbId = req.getParameter(0) ?? ''
    const method = req.getMethod()
    const path = req.getUrl()

    if (cors) {
      writeCorsOrigin(res, cors, req.getHeader('origin'))
    }

    const abort = initAbortHandler(res)
    const bodyPromise = readBody(res, maxBodyBytes, abort)

    if (!authenticateHook) {
      bodyPromise
        .then(async rawBody => {
          if (!abort.claim()) return
          try {
            await handler(res, dbId, rawBody, abort, undefined)
          } catch {
            if (!abort.aborted) {
              sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
            }
          }
        })
        .catch(() => {})
      return
    }

    const ctx: RequestContext = {
      headers: readHeaders(req),
      method,
      path,
      databaseId: dbId,
      remoteAddress: decodeRemoteAddress(res),
    }

    const hookPromise = runAuthenticate(res, abort, ctx, authenticateHook)

    Promise.all([bodyPromise, hookPromise])
      .then(async ([rawBody, authenticated]) => {
        if (!authenticated.ok || !abort.claim()) return
        try {
          await handler(res, dbId, rawBody, abort, authenticated.identity)
        } catch {
          if (!abort.aborted) {
            sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
          }
        }
      })
      .catch(() => {})
  }
}

export function wrapDbGetRoute<Identity>(
  deps: OperationRouteDeps<Identity>,
  handler: DbGetRouteHandler,
): (res: HttpResponse, req: HttpRequest) => void {
  const { authenticateHook, cors } = deps

  return (res, req) => {
    const dbId = req.getParameter(0) ?? ''
    const method = req.getMethod()
    const path = req.getUrl()

    if (cors) {
      writeCorsOrigin(res, cors, req.getHeader('origin'))
    }

    const ctx: RequestContext = {
      headers: readHeaders(req),
      method,
      path,
      databaseId: dbId,
      remoteAddress: decodeRemoteAddress(res),
    }

    const abort = initAbortHandler(res)
    const run = async (): Promise<void> => {
      if (authenticateHook && !(await runAuthenticate(res, abort, ctx, authenticateHook)).ok) return
      if (!abort.claim()) return
      try {
        await handler(res, dbId, ctx, abort)
      } catch {
        if (!abort.aborted) {
          sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
        }
      }
    }
    run().catch(() => {})
  }
}
