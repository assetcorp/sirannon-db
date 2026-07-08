import type { HttpResponse } from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  ClusterStatusInfo,
  ReadConcern,
  ServerExecutionTarget,
  ServerExecutionTargetResolver,
  WriteConcern,
} from '../core/types.js'
import type {
  ClusterStatusResponse,
  ErrorResponse,
  ExecuteRequest,
  QueryRequest,
  TransactionRequest,
} from './protocol.js'
import { toExecuteResponse } from './protocol.js'

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

function parseBody<T>(res: HttpResponse, raw: Buffer): T | null {
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

function sendJson(res: HttpResponse, data: unknown): void {
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

function httpStatusForError(err: SirannonError): number {
  switch (err.code) {
    case 'DATABASE_NOT_FOUND':
      return 404
    case 'READ_ONLY':
      return 403
    case 'QUERY_ERROR':
    case 'TRANSACTION_ERROR':
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

export type DbRouteHandler = (res: HttpResponse, dbId: string, rawBody: Buffer, abort: ResponseAbort) => Promise<void>

type ParseResult<T> = { ok: true; value: T | undefined } | { ok: false }

async function resolveExecutionTarget(
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

export function handleQuery(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<QueryRequest>(res, rawBody)
    if (!body) return

    if (!body.sql || typeof body.sql !== 'string') {
      sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
      return
    }

    const readConcern = parseReadConcern(res, body.readConcern)
    if (!readConcern.ok) return

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const rows = await target.query(
        body.sql,
        body.params,
        readConcern.value ? { readConcern: readConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, { rows })
    } catch (err) {
      if (abort.aborted) return
      if (err instanceof SirannonError) {
        sendError(res, httpStatusForError(err), err.code, err.message, errorDetails(err))
      } else {
        sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
      }
    }
  }
}

export function handleExecute(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<ExecuteRequest>(res, rawBody)
    if (!body) return

    if (!body.sql || typeof body.sql !== 'string') {
      sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
      return
    }

    const writeConcern = parseWriteConcern(res, body.writeConcern)
    if (!writeConcern.ok) return

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const result = await target.execute(
        body.sql,
        body.params,
        writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, toExecuteResponse(result))
    } catch (err) {
      if (abort.aborted) return
      if (err instanceof SirannonError) {
        sendError(res, httpStatusForError(err), err.code, err.message, errorDetails(err))
      } else {
        sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
      }
    }
  }
}

export function handleTransaction(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<TransactionRequest>(res, rawBody)
    if (!body) return

    if (!Array.isArray(body.statements)) {
      sendError(res, 400, 'INVALID_REQUEST', 'Field "statements" is required and must be an array')
      return
    }

    if (body.statements.length === 0) {
      sendError(res, 400, 'INVALID_REQUEST', 'Transaction requires at least one statement')
      return
    }

    const writeConcern = parseWriteConcern(res, body.writeConcern)
    if (!writeConcern.ok) return

    for (let i = 0; i < body.statements.length; i++) {
      const stmt = body.statements[i]
      if (!stmt.sql || typeof stmt.sql !== 'string') {
        sendError(res, 400, 'INVALID_REQUEST', `Statement at index ${i} is missing a valid "sql" field`)
        return
      }
    }

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const results = await target.transaction(
        async tx => {
          const txResults = []
          for (const stmt of body.statements) {
            if (abort.aborted) throw new Error('Request aborted')
            txResults.push(await tx.execute(stmt.sql, stmt.params))
          }
          return txResults
        },
        writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, {
        results: results.map(toExecuteResponse),
      })
    } catch (err) {
      if (abort.aborted) return
      if (err instanceof SirannonError) {
        sendError(res, httpStatusForError(err), err.code, err.message, errorDetails(err))
      } else {
        sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
      }
    }
  }
}

function parseReadConcern(res: HttpResponse, value: unknown): ParseResult<ReadConcern> {
  if (value === undefined) return { ok: true, value: undefined }
  if (!isPlainRecord(value)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "readConcern" must be an object when provided')
    return { ok: false }
  }
  const keys = Object.keys(value)
  if (keys.length !== 1 || !keys.includes('level')) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "readConcern" must contain only "level"')
    return { ok: false }
  }
  if (!isReadConcernLevel(value.level)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "readConcern.level" is invalid')
    return { ok: false }
  }
  return { ok: true, value: { level: value.level } }
}

function parseWriteConcern(res: HttpResponse, value: unknown): ParseResult<WriteConcern> {
  if (value === undefined) return { ok: true, value: undefined }
  if (!isPlainRecord(value)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "writeConcern" must be an object when provided')
    return { ok: false }
  }
  const allowedKeys = new Set(['level', 'timeoutMs'])
  if (!Object.keys(value).every(key => allowedKeys.has(key))) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "writeConcern" contains unsupported keys')
    return { ok: false }
  }
  if (!isWriteConcernLevel(value.level)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "writeConcern.level" is invalid')
    return { ok: false }
  }
  const timeoutMs = value.timeoutMs
  if (
    timeoutMs !== undefined &&
    (typeof timeoutMs !== 'number' || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0)
  ) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "writeConcern.timeoutMs" must be a positive safe integer')
    return { ok: false }
  }
  return {
    ok: true,
    value: timeoutMs === undefined ? { level: value.level } : { level: value.level, timeoutMs },
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isReadConcernLevel(value: unknown): value is ReadConcern['level'] {
  return value === 'local' || value === 'majority' || value === 'linearizable'
}

function isWriteConcernLevel(value: unknown): value is WriteConcern['level'] {
  return value === 'local' || value === 'majority' || value === 'all'
}

export function handleClusterStatus(getClusterStatus?: (databaseId: string) => ClusterStatusInfo | null) {
  return (res: HttpResponse, dbId: string): void => {
    if (!getClusterStatus) {
      sendError(res, 404, 'NOT_FOUND', 'Cluster status is not configured')
      return
    }
    const status = getClusterStatus(dbId)
    if (!status) {
      sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
      return
    }
    sendJson(res, toClusterStatusResponse(status))
  }
}

function toClusterStatusResponse(status: ClusterStatusInfo): ClusterStatusResponse {
  return {
    ...status,
    currentPrimary: status.currentPrimary ? { ...status.currentPrimary } : status.currentPrimary,
    readEndpoints: status.readEndpoints?.map(endpoint => ({
      ...endpoint,
      readConcerns: [...endpoint.readConcerns],
    })),
    primaryTerm: status.primaryTerm?.toString(),
  }
}

function errorDetails(err: SirannonError): Record<string, unknown> | undefined {
  const details = (err as SirannonError & { details?: Record<string, unknown> }).details
  return details && Object.keys(details).length > 0 ? details : undefined
}
