import type { HttpRequest, HttpResponse } from 'uWebSockets.js'
import { SirannonError, WriteOverloadError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ReadConcern, ServerExecutionTarget, ServerExecutionTargetResolver, WriteConcern } from '../core/types.js'
import { clientErrorMessage, sharesErrorDetails } from './client-error.js'
import { endResponse, markBodyArriving, settleResponse } from './connection-drain.js'
import type { ErrorResponse } from './protocol.js'
import { validateReadConcern, validateWriteConcern } from './protocol.js'

export const SQL_NOT_ACCEPTED_MESSAGE =
  'This server does not accept SQL statements over the wire; call a registered operation by name'

export const TABLE_SUBSCRIPTION_NOT_ACCEPTED_MESSAGE =
  'This server keeps table subscriptions off; subscribe to a registered read by name, or give the server acceptSql: true or an onBeforeSubscribe hook'

export const DEVICE_SYNC_NOT_ACCEPTED_MESSAGE =
  'This server keeps device sync off; the server has to set acceptDeviceSync before a device can sync'

export interface ResponseAbort {
  readonly aborted: boolean
  onAbort(fn: () => void): void
}

export interface ResponseGuard extends ResponseAbort {
  claim(): boolean
}

export function initAbortHandler(res: HttpResponse): ResponseGuard {
  const listeners: (() => void)[] = []
  let aborted = false
  let claimed = false

  res.onAborted(() => {
    aborted = true
    settleResponse(res)
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
    claim() {
      if (aborted || claimed) return false
      claimed = true
      return true
    },
  }
}

export function readBody(res: HttpResponse, maxBytes: number, abort: ResponseGuard): Promise<Buffer> {
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

    markBodyArriving(res, true)
    res.onData((chunk, isLast) => {
      if (done || abort.aborted) return

      totalLength += chunk.byteLength
      if (totalLength > maxBytes) {
        done = true
        if (abort.claim()) {
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
        markBodyArriving(res, false)
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

  let parsed: unknown
  try {
    parsed = JSON.parse(raw.toString('utf-8'))
  } catch {
    sendError(res, 400, 'INVALID_JSON', 'Request body is not valid JSON')
    return null
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Request body must be a JSON object')
    return null
  }
  return parsed as T
}

export function sendJson(res: HttpResponse, data: unknown, status = '200 OK'): void {
  const payload = JSON.stringify(data)
  endResponse(res, () => {
    res.writeStatus(status).writeHeader('Content-Type', 'application/json').end(payload)
  })
}

export function sendError(
  res: HttpResponse,
  status: number,
  code: string,
  message: string,
  details?: Record<string, unknown>,
  headers?: Record<string, string>,
): void {
  const body: ErrorResponse = { error: details ? { code, message, details } : { code, message } }
  const payload = JSON.stringify(body)
  endResponse(res, () => {
    res.writeStatus(`${status}`)
    if (headers) {
      for (const [name, value] of Object.entries(headers)) res.writeHeader(name, value)
    }
    res.writeHeader('Content-Type', 'application/json').end(payload)
  })
}

export function httpStatusForError(err: SirannonError): number {
  const explicit = (err as SirannonError & { status?: number }).status
  if (typeof explicit === 'number') return explicit

  switch (err.code) {
    case 'DATABASE_NOT_FOUND':
      return 404
    case 'READ_ONLY':
    case 'FORBIDDEN_SQL':
      return 403
    case 'QUERY_ERROR':
    case 'TRANSACTION_ERROR':
    case 'INVALID_DURABILITY':
    case 'INVALID_SYNCHRONOUS':
    case 'BATCH_VALIDATION_ERROR':
      return 400
    case 'STALE_PRIMARY':
    case 'PROTOCOL_VERSION_MISMATCH':
      return 409
    case 'HOOK_DENIED':
      return 403
    case 'BACKUP_CHAIN_BROKEN':
      return 409
    case 'BACKUP_UNSUPPORTED':
      return 501
    case 'BACKUP_DESTINATION_ERROR':
      return 502
    case 'DATABASE_CLOSED':
    case 'SHUTDOWN':
    case 'READ_CONCERN_ERROR':
    case 'COORDINATOR_UNAVAILABLE':
    case 'AUTHORITY_LOST':
    case 'NO_SAFE_PRIMARY':
    case 'NODE_NOT_IN_SYNC':
    case 'NODE_DRAINING':
    case 'UNSAFE_RECOVERY_REQUIRED':
    case 'WRITE_OVERLOADED':
      return 503
    default:
      return 500
  }
}

export function errorDetails(err: SirannonError): Record<string, unknown> | undefined {
  if (!sharesErrorDetails(err)) return undefined
  const details = (err as SirannonError & { details?: Record<string, unknown> }).details
  return details && Object.keys(details).length > 0 ? details : undefined
}

export function sendCaughtError(res: HttpResponse, abort: ResponseAbort, err: unknown): void {
  if (abort.aborted) return
  if (err instanceof SirannonError) {
    const headers =
      err instanceof WriteOverloadError ? { 'Retry-After': `${Math.ceil(err.retryAfterMs / 1000)}` } : undefined
    sendError(res, httpStatusForError(err), err.code, clientErrorMessage(err), errorDetails(err), headers)
  } else {
    sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
  }
}

const DATABASE_ID_SEGMENT = 'Database identifier'

const OPERATION_NAME_SEGMENT = 'Operation name'

/**
 * One named segment of a request path, such as the database identifier in `/db/{id}`, as sent and as decoded.
 *
 * @internal
 */
export interface PathSegment {
  /** The segment as the request sent it, which the `authenticate` hook receives when the server refuses the segment. */
  readonly raw: string
  /** The percent-decoded segment, or `null` when the server refuses the segment. */
  readonly decoded: string | null
  /** The name of the segment that an error message uses, such as `Database identifier`. */
  readonly field: string
}

const PATH_SEPARATOR = /[/\\]/

const ROOTED_PATH = /^(?:[/\\]|[A-Za-z]:)/

function staysInsideDirectory(value: string): boolean {
  if (ROOTED_PATH.test(value)) return false
  return !value.split(PATH_SEPARATOR).some(part => part === '.' || part === '..')
}

function percentDecode(raw: string): string | null {
  if (!raw.includes('%')) return raw
  try {
    return decodeURIComponent(raw)
  } catch {
    return null
  }
}

/**
 * Reads the database identifier that the first parameter of a `/db/{id}` route carries, and percent-decodes it.
 *
 * The decoded value is `null` when the identifier is not valid percent-encoding, when it decodes to a value that starts
 * with a slash, a backslash, or a drive letter and a colon, or when one of the parts that a slash or a backslash
 * separates is `.` or `..`. A route refuses such an identifier, so that an `autoOpen` resolver or an execution-target
 * resolver that joins the identifier onto a directory can only name a file inside that directory. An identifier such
 * as `a/b` still decodes, because the registry accepts it.
 *
 * @param req - The request whose route matched.
 * @returns The identifier as sent, and the decoded identifier or `null` when the route refuses it.
 * @internal
 */
export function readDatabaseId(req: HttpRequest): PathSegment {
  const raw = req.getParameter(0) ?? ''
  const decoded = percentDecode(raw)
  const nameable = decoded !== null && staysInsideDirectory(decoded)
  return { raw, decoded: nameable ? decoded : null, field: DATABASE_ID_SEGMENT }
}

/**
 * Reads the operation name that the second parameter of a `/db/{id}/query/{name}` or `/db/{id}/execute/{name}` route
 * carries, and percent-decodes it.
 *
 * The server looks a name up in its registry and never builds a path from it, so a name keeps every character that it
 * decodes to, slashes included.
 *
 * @param req - The request whose route matched.
 * @returns The name as sent, and the decoded name or `null` when the name is not valid percent-encoding.
 * @internal
 */
export function readOperationName(req: HttpRequest): PathSegment {
  const raw = req.getParameter(1) ?? ''
  return { raw, decoded: percentDecode(raw), field: OPERATION_NAME_SEGMENT }
}

function refusalMessage(segment: PathSegment): string {
  if (percentDecode(segment.raw) === null) return `${segment.field} '${segment.raw}' is not valid percent-encoding`
  return `${segment.field} '${segment.raw}' decodes to a path that can leave its directory, because it starts with a slash, a backslash, or a drive letter, or because one of its parts is '.' or '..'`
}

/**
 * Answers `400 INVALID_REQUEST` for the first segment that {@link readDatabaseId} or {@link readOperationName} refused.
 *
 * @param res - The response of the request.
 * @param segments - The segments of the request path, in the order that the route pattern names them.
 * @returns `true` when the function answered the request, and `false` when the server accepts every segment.
 * @internal
 */
export function refuseUndecodablePathSegments(res: HttpResponse, segments: readonly PathSegment[]): boolean {
  for (const segment of segments) {
    if (segment.decoded === null) {
      sendError(res, 400, 'INVALID_REQUEST', refusalMessage(segment))
      return true
    }
  }
  return false
}

export type ParseResult<T> = { ok: true; value: T | undefined } | { ok: false }

export async function resolveExecutionTarget(
  res: HttpResponse,
  abort: ResponseAbort,
  sirannon: Sirannon,
  id: string,
  resolver?: ServerExecutionTargetResolver,
): Promise<ServerExecutionTarget | null> {
  let target: ServerExecutionTarget | null | undefined
  try {
    target = resolver ? await resolver(id) : await sirannon.resolve(id)
  } catch (err) {
    if (abort.aborted) return null
    if (err instanceof SirannonError) {
      sendError(res, httpStatusForError(err), err.code, clientErrorMessage(err))
    } else {
      sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
    }
    return null
  }
  if (abort.aborted) return null
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
