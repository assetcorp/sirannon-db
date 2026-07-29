import type uWS from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import type { ResponseGuard } from './http-common.js'
import { sendCaughtError, sendError } from './http-common.js'

export function decodeRemoteAddress(res: uWS.HttpResponse): string {
  return Buffer.from(res.getRemoteAddressAsText()).toString()
}

export type AuthenticationResult<I> = { ok: true; identity: I | undefined } | { ok: false }

export const RETURNED_REFUSAL_MESSAGE =
  'The authenticate hook returned a refusal object rather than an identity. A hook refuses a request by throwing a SirannonError; every value it returns becomes the caller identity.'

function looksLikeRefusal(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) return false
  const candidate = value as Record<string, unknown>
  return (
    typeof candidate.status === 'number' && typeof candidate.code === 'string' && typeof candidate.message === 'string'
  )
}

export async function runAuthenticate<I>(
  res: uWS.HttpResponse,
  abort: ResponseGuard,
  ctx: RequestContext,
  hook: AuthenticateHook<I>,
): Promise<AuthenticationResult<I>> {
  try {
    const identity = await hook(ctx)
    if (looksLikeRefusal(identity)) {
      if (abort.claim()) sendError(res, 500, 'HOOK_ERROR', RETURNED_REFUSAL_MESSAGE)
      return { ok: false }
    }
    return { ok: true, identity: identity as I | undefined }
  } catch (err) {
    if (abort.claim()) {
      if (err instanceof SirannonError) {
        sendCaughtError(res, abort, err)
      } else {
        sendError(res, 500, 'HOOK_ERROR', 'authenticate hook threw an error')
      }
    }
    return { ok: false }
  }
}
