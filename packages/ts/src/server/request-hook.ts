import type uWS from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import { WS_CLOSE_FORBIDDEN, WS_CLOSE_UNAUTHENTICATED } from '../core/ws-handshake.js'
import type { ResponseGuard } from './http-common.js'
import { httpStatusForError, sendCaughtError, sendError } from './http-common.js'

export function decodeRemoteAddress(res: uWS.HttpResponse): string {
  return Buffer.from(res.getRemoteAddressAsText()).toString()
}

export type AuthenticationResult<I> = { ok: true; identity: I | undefined } | { ok: false }

export interface UpgradeRefusal {
  code: number
  reason: string
}

export type UpgradeAuthenticationResult<I> =
  | { ok: true; identity: I | undefined }
  | { ok: false; refusal: UpgradeRefusal | undefined }

type HookOutcome<I> =
  | { status: 'identity'; identity: I | undefined }
  | { status: 'threw'; error: unknown }
  | { status: 'returned-refusal' }

const CLOSE_REASON_MAX_BYTES = 123

export const RETURNED_REFUSAL_MESSAGE =
  'The authenticate hook returned a refusal object rather than an identity. A hook refuses a request by throwing a SirannonError; every value it returns becomes the caller identity.'

function looksLikeRefusal(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) return false
  const candidate = value as Record<string, unknown>
  return (
    typeof candidate.status === 'number' && typeof candidate.code === 'string' && typeof candidate.message === 'string'
  )
}

async function invokeAuthenticate<I>(ctx: RequestContext, hook: AuthenticateHook<I>): Promise<HookOutcome<I>> {
  try {
    const identity = await hook(ctx)
    if (looksLikeRefusal(identity)) return { status: 'returned-refusal' }
    return { status: 'identity', identity: identity as I | undefined }
  } catch (error) {
    return { status: 'threw', error }
  }
}

function sendRefusal<I>(res: uWS.HttpResponse, abort: ResponseGuard, outcome: HookOutcome<I>): void {
  if (!abort.claim()) return

  if (outcome.status === 'returned-refusal') {
    sendError(res, 500, 'HOOK_ERROR', RETURNED_REFUSAL_MESSAGE)
    return
  }

  if (outcome.status === 'threw' && outcome.error instanceof SirannonError) {
    sendCaughtError(res, abort, outcome.error)
    return
  }

  sendError(res, 500, 'HOOK_ERROR', 'authenticate hook threw an error')
}

function truncateCloseReason(reason: string): string {
  if (Buffer.byteLength(reason, 'utf-8') <= CLOSE_REASON_MAX_BYTES) return reason

  let truncated = reason.slice(0, CLOSE_REASON_MAX_BYTES)
  while (truncated.length > 0 && Buffer.byteLength(truncated, 'utf-8') > CLOSE_REASON_MAX_BYTES) {
    truncated = truncated.slice(0, -1)
  }
  return truncated
}

function upgradeRefusalFor(error: unknown): UpgradeRefusal | undefined {
  if (!(error instanceof SirannonError)) return undefined

  const status = httpStatusForError(error)
  if (status !== 401 && status !== 403) return undefined

  return {
    code: status === 403 ? WS_CLOSE_FORBIDDEN : WS_CLOSE_UNAUTHENTICATED,
    reason: truncateCloseReason(`${error.code}: ${error.message}`),
  }
}

export async function runAuthenticate<I>(
  res: uWS.HttpResponse,
  abort: ResponseGuard,
  ctx: RequestContext,
  hook: AuthenticateHook<I>,
): Promise<AuthenticationResult<I>> {
  const outcome = await invokeAuthenticate(ctx, hook)
  if (outcome.status === 'identity') return { ok: true, identity: outcome.identity }
  sendRefusal(res, abort, outcome)
  return { ok: false }
}

export async function authenticateUpgrade<I>(
  res: uWS.HttpResponse,
  abort: ResponseGuard,
  ctx: RequestContext,
  hook: AuthenticateHook<I>,
): Promise<UpgradeAuthenticationResult<I>> {
  const outcome = await invokeAuthenticate(ctx, hook)
  if (outcome.status === 'identity') return { ok: true, identity: outcome.identity }

  if (outcome.status === 'threw') {
    const refusal = upgradeRefusalFor(outcome.error)
    if (refusal) return { ok: false, refusal }
  }

  sendRefusal(res, abort, outcome)
  return { ok: false, refusal: undefined }
}
