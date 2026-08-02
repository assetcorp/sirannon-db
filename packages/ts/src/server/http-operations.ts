import type { HttpResponse } from 'uWebSockets.js'
import type { OperationArguments, OperationRegistry, OperationStatement } from '../core/operation-registry.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ServerExecutionTargetResolver } from '../core/types.js'
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
import { findRead, findWrite, isRefusal, resolveArguments } from './operation-lookup.js'
import { decodeBoundParams, toExecuteResponse } from './protocol.js'
import { queryWireRows } from './wire-rows.js'

export interface OperationRequest {
  args?: Record<string, unknown>
  readConcern?: unknown
  writeConcern?: unknown
}

export type OperationRouteHandler = (
  res: HttpResponse,
  dbId: string,
  name: string,
  identity: unknown,
  rawBody: Buffer,
  abort: ResponseAbort,
) => Promise<void>

function parseOperationBody(res: HttpResponse, rawBody: Buffer): OperationRequest | null {
  if (rawBody.length === 0) return {}
  return parseBody<OperationRequest>(res, rawBody)
}

function decodeArguments(res: HttpResponse, raw: Record<string, unknown> | undefined): OperationArguments | null {
  if (raw === undefined) return {}
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    sendError(res, 400, 'INVALID_REQUEST', 'Field "args" must be an object')
    return null
  }
  const decoded = decodeBoundParams(raw, 'args')
  if (!decoded.ok) {
    sendError(res, 400, 'INVALID_REQUEST', decoded.message)
    return null
  }
  return (decoded.value ?? {}) as OperationArguments
}

export function handleOperationQuery<I>(
  sirannon: Sirannon,
  registry: OperationRegistry<I> | undefined,
  resolveTarget?: ServerExecutionTargetResolver,
): OperationRouteHandler {
  return async (res, dbId, name, identity, rawBody, abort) => {
    const operation = findRead(registry, dbId, name)
    if (isRefusal(operation)) {
      sendError(res, operation.status, operation.code, operation.message)
      return
    }

    const body = parseOperationBody(res, rawBody)
    if (!body) return

    const readConcern = parseReadConcern(res, body.readConcern)
    if (!readConcern.ok) return

    const supplied = decodeArguments(res, body.args)
    if (!supplied) return

    const args = resolveArguments<I>(operation, supplied, identity as I | undefined)
    if (!args.ok) {
      sendError(res, args.refusal.status, args.refusal.code, args.refusal.message)
      return
    }

    let statement: OperationStatement
    try {
      statement = operation.statement(args.value)
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }

    const target = await resolveExecutionTarget(res, abort, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const rows = await queryWireRows(
        target,
        statement.sql,
        statement.params,
        readConcern.value ? { readConcern: readConcern.value } : undefined,
      )
      if (abort.aborted) return
      sendJson(res, { rows })
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}

export function handleOperationExecute<I>(
  sirannon: Sirannon,
  registry: OperationRegistry<I> | undefined,
  resolveTarget?: ServerExecutionTargetResolver,
): OperationRouteHandler {
  return async (res, dbId, name, identity, rawBody, abort) => {
    const operation = findWrite(registry, dbId, name)
    if (isRefusal(operation)) {
      sendError(res, operation.status, operation.code, operation.message)
      return
    }

    const body = parseOperationBody(res, rawBody)
    if (!body) return

    const writeConcern = parseWriteConcern(res, body.writeConcern)
    if (!writeConcern.ok) return

    const supplied = decodeArguments(res, body.args)
    if (!supplied) return

    const args = resolveArguments<I>(operation, supplied, identity as I | undefined)
    if (!args.ok) {
      sendError(res, args.refusal.status, args.refusal.code, args.refusal.message)
      return
    }

    let statements: readonly OperationStatement[]
    try {
      const produced = operation.statements(args.value)
      statements = Array.isArray(produced) ? produced : [produced as OperationStatement]
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }

    if (statements.length === 0) {
      sendError(res, 500, 'INTERNAL_ERROR', `Operation '${name}' produced no statements`)
      return
    }

    const target = await resolveExecutionTarget(res, abort, sirannon, dbId, resolveTarget)
    if (!target) return

    const txOptions = writeConcern.value ? { writeConcern: writeConcern.value } : undefined
    try {
      const results = target.executeTransaction
        ? await target.executeTransaction(statements, txOptions)
        : await target.transaction(async tx => {
            const txResults = []
            for (const statement of statements) {
              txResults.push(await tx.execute(statement.sql, statement.params))
            }
            return txResults
          }, txOptions)
      if (abort.aborted) return
      sendJson(res, { results: results.map(toExecuteResponse) })
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
