import type { HttpResponse } from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ClusterStatusInfo, ServerExecutionTargetResolver } from '../core/types.js'
import type { ResponseAbort } from './http-common.js'
import {
  errorDetails,
  httpStatusForError,
  parseBody,
  parseReadConcern,
  parseWriteConcern,
  resolveExecutionTarget,
  sendError,
  sendJson,
} from './http-common.js'
import type {
  BatchRequest,
  ClusterStatusResponse,
  ExecuteRequest,
  LoadRequest,
  QueryRequest,
  TransactionRequest,
} from './protocol.js'
import {
  loadDurabilityValidationError,
  paramsBatchValidationError,
  toExecuteResponse,
  toLoadResponse,
} from './protocol.js'

export type { ResponseAbort } from './http-common.js'
export { initAbortHandler, readBody, sendError } from './http-common.js'

export type DbRouteHandler = (res: HttpResponse, dbId: string, rawBody: Buffer, abort: ResponseAbort) => Promise<void>

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

export function handleBatch(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<BatchRequest>(res, rawBody)
    if (!body) return

    if (!body.sql || typeof body.sql !== 'string') {
      sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
      return
    }

    const paramsBatchError = paramsBatchValidationError(body.paramsBatch)
    if (paramsBatchError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', paramsBatchError)
      return
    }

    const writeConcern = parseWriteConcern(res, body.writeConcern)
    if (!writeConcern.ok) return

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const results = await target.transaction(
        async tx => tx.executeBatch(body.sql, body.paramsBatch),
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

export function handleLoad(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<LoadRequest>(res, rawBody)
    if (!body) return

    if (!body.sql || typeof body.sql !== 'string') {
      sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
      return
    }

    const paramsBatchError = paramsBatchValidationError(body.paramsBatch)
    if (paramsBatchError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', paramsBatchError)
      return
    }

    const durabilityError = loadDurabilityValidationError(body.durability)
    if (durabilityError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', durabilityError)
      return
    }

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    const bulkLoad = target.bulkLoad
    if (typeof bulkLoad !== 'function') {
      sendError(res, 501, 'BULK_LOAD_UNSUPPORTED', 'The execution target for this database does not support bulk load')
      return
    }

    try {
      const results = await bulkLoad.call(
        target,
        body.sql,
        body.paramsBatch,
        body.durability ? { durability: body.durability } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, toLoadResponse(results))
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
