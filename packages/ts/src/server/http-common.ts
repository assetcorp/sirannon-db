import type { HttpResponse } from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ReadConcern, ServerExecutionTarget, ServerExecutionTargetResolver, WriteConcern } from '../core/types.js'
import type { ErrorResponse } from './protocol.js'
import { validateReadConcern, validateWriteConcern } from './protocol.js'

/** Abort tracking for a uWS response so late writes never touch a dead socket. */
export interface ResponseAbort {
  readonly aborted: boolean
  onAbort(fn: () => void): void
}

export function initAbortHandler(res: HttpResponse): ResponseAbort {
  const listeners: (() => void)[] = []
  let aborted = false

  res.onAborted(() => {
    aborted = true
    for (const fn of listeners) fn()
  })

  return {
    get aborted() {
      return aborted
    },
    onAbort(fn) {
      if (aborted) {
        fn()
      } else {
        listeners.push(fn)
      }
    },
  }
}

/**
 * Read the request body as it streams in, enforcing `maxBytes` per chunk so
 * an oversized request is rejected with 413 before it is ever fully buffered.
 */
export function readBody(res: HttpResponse, maxBytes: number, abort: ResponseAbort): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    if (abort.aborted) {
      reject(new Error('Request aborted'))
      return
    }

    let done = false
    const chunks: Buffer[] = []
    let totalLength = 0

    abort.onAbort(() => {
      if (done) return
      done = true
      reject(new Error('Request aborted'))
    })

    res.onData((chunk, isLast) => {
      if (done || abort.aborted) return

      totalLength += chunk.byteLength
      if (totalLength > maxBytes) {
        done = true
        if (!abort.aborted) {
          sendError(res, 413, 'PAYLOAD_TOO_LARGE', 'Request body exceeds size limit')
        }
        reject(new Error('Payload too large'))
        return
      }

      const bytes = new Uint8Array(chunk)
      const copy = Buffer.allocUnsafe(bytes.byteLength)
      copy.set(bytes)
      chunks.push(copy)

      if (isLast) {
        done = true
        resolve(Buffer.concat(chunks))
      }
    })
  })
}

export function parseBody<T>(res: HttpResponse, raw: Buffer): T | null {
  if (raw.length === 0) {
    sendError(res, 400, 'EMPTY_BODY', 'Request body is empty')
    return null
  }

  try {
    return JSON.parse(raw.toString('utf-8')) as T
  } catch {
    sendError(res, 400, 'INVALID_JSON', 'Request body is not valid JSON')
    return null
  }
}

export function sendJson(res: HttpResponse, data: unknown): void {
  const payload = JSON.stringify(data)
  res.cork(() => {
    res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
  })
}

export function sendError(
  res: HttpResponse,
  status: number,
  code: string,
  message: string,
  details?: Record<string, unknown>,
): void {
  const body: ErrorResponse = { error: details ? { code, message, details } : { code, message } }
  const payload = JSON.stringify(body)
  res.cork(() => {
    res.writeStatus(`${status}`).writeHeader('Content-Type', 'application/json').end(payload)
  })
}

export function httpStatusForError(err: SirannonError): number {
  switch (err.code) {
    case 'DATABASE_NOT_FOUND':
      return 404
    case 'READ_ONLY':
      return 403
    case 'QUERY_ERROR':
    case 'TRANSACTION_ERROR':
    case 'INVALID_DURABILITY':
    case 'INVALID_SYNCHRONOUS':
      return 400
    case 'STALE_PRIMARY':
    case 'PROTOCOL_VERSION_MISMATCH':
      return 409
    case 'HOOK_DENIED':
      return 403
    case 'DATABASE_CLOSED':
    case 'SHUTDOWN':
    case 'READ_CONCERN_ERROR':
    case 'COORDINATOR_UNAVAILABLE':
    case 'AUTHORITY_LOST':
    case 'NO_SAFE_PRIMARY':
    case 'NODE_NOT_IN_SYNC':
    case 'NODE_DRAINING':
    case 'UNSAFE_RECOVERY_REQUIRED':
      return 503
    default:
      return 500
  }
}

export function errorDetails(err: SirannonError): Record<string, unknown> | undefined {
  const details = (err as SirannonError & { details?: Record<string, unknown> }).details
  return details && Object.keys(details).length > 0 ? details : undefined
}

/** Map a thrown value to the response, unless the socket already aborted. */
export function sendCaughtError(res: HttpResponse, abort: ResponseAbort, err: unknown): void {
  if (abort.aborted) return
  if (err instanceof SirannonError) {
    sendError(res, httpStatusForError(err), err.code, err.message, errorDetails(err))
  } else {
    sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
  }
}

export type ParseResult<T> = { ok: true; value: T | undefined } | { ok: false }

export async function resolveExecutionTarget(
  res: HttpResponse,
  sirannon: Sirannon,
  id: string,
  resolver?: ServerExecutionTargetResolver,
): Promise<ServerExecutionTarget | null> {
  let target: ServerExecutionTarget | null | undefined
  try {
    target = resolver ? await resolver(id) : await sirannon.resolve(id)
  } catch (err) {
    if (err instanceof SirannonError) {
      sendError(res, httpStatusForError(err), err.code, err.message)
    } else {
      sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
    }
    return null
  }
  if (!target) {
    sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${id}' not found`)
    return null
  }
  return target
}

export function parseReadConcern(res: HttpResponse, value: unknown): ParseResult<ReadConcern> {
  const validation = validateReadConcern(value)
  if (!validation.ok) {
    sendError(res, 400, 'INVALID_REQUEST', validation.message)
    return { ok: false }
  }
  return validation
}

export function parseWriteConcern(res: HttpResponse, value: unknown): ParseResult<WriteConcern> {
  const validation = validateWriteConcern(value)
  if (!validation.ok) {
    sendError(res, 400, 'INVALID_REQUEST', validation.message)
    return { ok: false }
  }
  return validation
}
