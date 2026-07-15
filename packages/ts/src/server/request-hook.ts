import type uWS from 'uWebSockets.js'
import type { OnRequestHook, RequestContext, RequestDenial } from '../core/types.js'
import type { ResponseAbort } from './http-common.js'
import { sendError } from './http-common.js'

export function decodeRemoteAddress(res: uWS.HttpResponse): string {
  return Buffer.from(res.getRemoteAddressAsText()).toString()
}

function isRequestDenial(value: unknown): value is RequestDenial {
  return typeof value === 'object' && value !== null && 'status' in value
}

export async function runOnRequest(
  res: uWS.HttpResponse,
  abort: ResponseAbort,
  ctx: RequestContext,
  hook: OnRequestHook,
): Promise<boolean> {
  try {
    const result = await hook(ctx)
    if (isRequestDenial(result)) {
      if (!abort.aborted) sendError(res, result.status, result.code, result.message)
      return false
    }
    return true
  } catch {
    if (!abort.aborted) sendError(res, 500, 'HOOK_ERROR', 'onRequest hook threw an error')
    return false
  }
}
