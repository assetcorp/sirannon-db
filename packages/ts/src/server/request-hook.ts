import type uWS from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import { WS_CLOSE_FORBIDDEN, WS_CLOSE_UNAUTHENTICATED } from '../core/ws-handshake.js'
import type { ResponseGuard } from './http-common.js'
import { errorDetails, sendError } from './http-common.js'

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

interface CarriedRefusal {
  status: number
  code: string
  message: string
  details: Record<string, unknown> | undefined
}

const LOWEST_ERROR_STATUS = 400
const HIGHEST_ERROR_STATUS = 599

function readCarriedRefusal(error: object): CarriedRefusal | undefined {
  const { status, code, message } = error as Record<string, unknown>
  if (typeof status !== 'number' || !Number.isInteger(status)) return undefined
  if (status < LOWEST_ERROR_STATUS || status > HIGHEST_ERROR_STATUS) return undefined
  if (typeof code !== 'string' || code.length === 0 || typeof message !== 'string') return undefined
  const details = error instanceof SirannonError ? errorDetails(error) : undefined
  return { status, code, message, details }
}

function carriedRefusalOf(error: unknown): CarriedRefusal | undefined {
  if (typeof error !== 'object' || error === null) return undefined
  try {
    return readCarriedRefusal(error)
  } catch {
    return undefined
  }
}

function sendRefusal<I>(res: uWS.HttpResponse, abort: ResponseGuard, outcome: HookOutcome<I>): void {
  if (!abort.claim()) return

  if (outcome.status === 'returned-refusal') {
    sendError(res, 500, 'HOOK_ERROR', RETURNED_REFUSAL_MESSAGE)
    return
  }

  const carried = outcome.status === 'threw' ? carriedRefusalOf(outcome.error) : undefined
  if (carried) {
    sendError(res, carried.status, carried.code, carried.message, carried.details)
    return
  }

  sendError(res, 500, 'HOOK_ERROR', 'authenticate hook threw an error')
}

const SURROGATE_FIRST = 0xd800
const SURROGATE_LAST = 0xdfff
const REPLACEMENT_CHARACTER = '\u{FFFD}'

function wellFormedCharacter(character: string): string {
  const codePoint = character.codePointAt(0) ?? 0
  return codePoint >= SURROGATE_FIRST && codePoint <= SURROGATE_LAST ? REPLACEMENT_CHARACTER : character
}

function truncateCloseReason(reason: string): string {
  let truncated = ''
  let bytes = 0
  for (const character of reason) {
    const kept = wellFormedCharacter(character)
    const size = Buffer.byteLength(kept, 'utf-8')
    if (bytes + size > CLOSE_REASON_MAX_BYTES) break
    truncated += kept
    bytes += size
  }
  return truncated
}

function upgradeRefusalFor(error: unknown): UpgradeRefusal | undefined {
  const carried = carriedRefusalOf(error)
  if (!carried || (carried.status !== 401 && carried.status !== 403)) return undefined

  return {
    code: carried.status === 403 ? WS_CLOSE_FORBIDDEN : WS_CLOSE_UNAUTHENTICATED,
    reason: truncateCloseReason(`${carried.code}: ${carried.message}`),
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
