import type { HttpResponse } from 'uWebSockets.js'
import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ClusterStatusInfo, ServerExecutionTargetResolver } from '../core/types.js'
import type { ResponseAbort } from './http-common.js'
import {
  parseBody,
  parseReadConcern,
  parseWriteConcern,
  resolveExecutionTarget,
  sendCaughtError,
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
  decodeBoundParams,
  loadDurabilityValidationError,
  paramsBatchValidationError,
  toExecuteResponse,
  transactionStatementsValidationError,
} from './protocol.js'

function decodeBatchParams(
  res: HttpResponse,
  raw: (Record<string, unknown> | unknown[])[],
): (Record<string, unknown> | unknown[])[] | null {
  const decoded: (Record<string, unknown> | unknown[])[] = []
  for (const entry of raw) {
    const result = decodeBoundParams(entry, 'paramsBatch')
    if (!result.ok) {
      sendError(res, 400, 'INVALID_REQUEST', result.message)
      return null
    }
    decoded.push(result.value as Record<string, unknown> | unknown[])
  }
  return decoded
}

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

    const params = decodeBoundParams(body.params, 'params')
    if (!params.ok) {
      sendError(res, 400, 'INVALID_REQUEST', params.message)
      return
    }

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const rows = await target.query(
        body.sql,
        params.value,
        readConcern.value ? { readConcern: readConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, { rows: encodeTaggedValues(rows) })
    } catch (err) {
      sendCaughtError(res, abort, err)
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

    const params = decodeBoundParams(body.params, 'params')
    if (!params.ok) {
      sendError(res, 400, 'INVALID_REQUEST', params.message)
      return
    }

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const result = await target.execute(
        body.sql,
        params.value,
        writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, toExecuteResponse(result))
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}

export function handleTransaction(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<TransactionRequest>(res, rawBody)
    if (!body) return

    const statementsError = transactionStatementsValidationError(body.statements)
    if (statementsError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', statementsError)
      return
    }

    const writeConcern = parseWriteConcern(res, body.writeConcern)
    if (!writeConcern.ok) return

    const statements: { sql: string; params?: Record<string, unknown> | unknown[] }[] = []
    for (const stmt of body.statements) {
      const params = decodeBoundParams(stmt.params, 'params')
      if (!params.ok) {
        sendError(res, 400, 'INVALID_REQUEST', params.message)
        return
      }
      statements.push({ sql: stmt.sql, params: params.value })
    }

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const results = await target.transaction(
        async tx => {
          const txResults = []
          for (const stmt of statements) {
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
      sendCaughtError(res, abort, err)
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

    const paramsBatch = decodeBatchParams(res, body.paramsBatch)
    if (paramsBatch === null) return

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const results = await target.transaction(
        async tx => tx.executeBatch(body.sql, paramsBatch),
        writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, {
        results: results.map(toExecuteResponse),
      })
    } catch (err) {
      sendCaughtError(res, abort, err)
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

    const paramsBatch = decodeBatchParams(res, body.paramsBatch)
    if (paramsBatch === null) return

    const target = await resolveExecutionTarget(res, sirannon, dbId, resolveTarget)
    if (!target) return

    const bulkLoad = target.bulkLoad
    if (typeof bulkLoad !== 'function') {
      sendError(res, 501, 'BULK_LOAD_UNSUPPORTED', 'The execution target for this database does not support bulk load')
      return
    }

    try {
      const summary = await bulkLoad.call(
        target,
        body.sql,
        paramsBatch,
        body.durability ? { durability: body.durability } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, summary)
    } catch (err) {
      sendCaughtError(res, abort, err)
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
