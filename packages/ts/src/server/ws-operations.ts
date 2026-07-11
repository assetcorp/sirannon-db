import type { ServerExecutionTarget } from '../core/types.js'
import type { WSResultMessage } from './protocol.js'
import {
  decodeBoundParams,
  loadDurabilityValidationError,
  paramsBatchValidationError,
  toExecuteResponse,
  transactionStatementsValidationError,
  validateWriteConcern,
} from './protocol.js'
import { queryWireRows } from './wire-rows.js'

/**
 * Reply surface handed to each data-operation message handler, so the
 * handlers stay independent of the connection bookkeeping in WSHandler.
 */
export interface WSOperationContext {
  target: ServerExecutionTarget
  sendResult(id: string, data: WSResultMessage['data']): void
  sendError(id: string, code: string, message: string): void
  sendCaughtError(id: string, err: unknown): void
}

export function isValidParams(params: unknown): boolean {
  if (params === undefined || params === null) return true
  return typeof params === 'object'
}

function decodeBatchParams(
  ctx: WSOperationContext,
  id: string,
  raw: (Record<string, unknown> | unknown[])[],
): (Record<string, unknown> | unknown[])[] | null {
  const decoded: (Record<string, unknown> | unknown[])[] = []
  for (const entry of raw) {
    const result = decodeBoundParams(entry, 'paramsBatch')
    if (!result.ok) {
      ctx.sendError(id, 'INVALID_MESSAGE', result.message)
      return null
    }
    decoded.push(result.value as Record<string, unknown> | unknown[])
  }
  return decoded
}

export async function handleQueryMessage(
  ctx: WSOperationContext,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (typeof msg.sql !== 'string') {
    ctx.sendError(id, 'INVALID_MESSAGE', 'Query message requires a "sql" string field')
    return
  }

  if (!isValidParams(msg.params)) {
    ctx.sendError(id, 'INVALID_MESSAGE', '"params" must be an object or array')
    return
  }

  const params = decodeBoundParams(msg.params, 'params')
  if (!params.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', params.message)
    return
  }

  try {
    const rows = await queryWireRows(ctx.target, msg.sql, params.value)
    ctx.sendResult(id, { rows: rows as Record<string, unknown>[] })
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}

export async function handleExecuteMessage(
  ctx: WSOperationContext,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (typeof msg.sql !== 'string') {
    ctx.sendError(id, 'INVALID_MESSAGE', 'Execute message requires a "sql" string field')
    return
  }

  if (!isValidParams(msg.params)) {
    ctx.sendError(id, 'INVALID_MESSAGE', '"params" must be an object or array')
    return
  }

  const params = decodeBoundParams(msg.params, 'params')
  if (!params.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', params.message)
    return
  }

  try {
    const result = await ctx.target.execute(msg.sql, params.value)
    ctx.sendResult(id, toExecuteResponse(result))
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}

export async function handleTransactionMessage(
  ctx: WSOperationContext,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  const statementsError = transactionStatementsValidationError(msg.statements)
  if (statementsError !== null) {
    ctx.sendError(id, 'INVALID_MESSAGE', statementsError)
    return
  }

  const writeConcern = validateWriteConcern(msg.writeConcern)
  if (!writeConcern.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', writeConcern.message)
    return
  }

  const validStatements = msg.statements as { sql: string; params?: Record<string, unknown> | unknown[] }[]
  const statements: { sql: string; params?: Record<string, unknown> | unknown[] }[] = []
  for (const stmt of validStatements) {
    const params = decodeBoundParams(stmt.params, 'params')
    if (!params.ok) {
      ctx.sendError(id, 'INVALID_MESSAGE', params.message)
      return
    }
    statements.push({ sql: stmt.sql, params: params.value })
  }

  try {
    const results = await ctx.target.transaction(
      async tx => {
        const txResults = []
        for (const stmt of statements) {
          txResults.push(await tx.execute(stmt.sql, stmt.params))
        }
        return txResults
      },
      writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
    )
    ctx.sendResult(id, { results: results.map(toExecuteResponse) })
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}

export async function handleBatchMessage(
  ctx: WSOperationContext,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (typeof msg.sql !== 'string') {
    ctx.sendError(id, 'INVALID_MESSAGE', 'Batch message requires a "sql" string field')
    return
  }

  const paramsBatchError = paramsBatchValidationError(msg.paramsBatch)
  if (paramsBatchError !== null) {
    ctx.sendError(id, 'INVALID_MESSAGE', paramsBatchError)
    return
  }

  const writeConcern = validateWriteConcern(msg.writeConcern)
  if (!writeConcern.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', writeConcern.message)
    return
  }

  const sql = msg.sql
  const paramsBatch = decodeBatchParams(ctx, id, msg.paramsBatch as (Record<string, unknown> | unknown[])[])
  if (paramsBatch === null) return

  try {
    const results = await ctx.target.transaction(
      async tx => tx.executeBatch(sql, paramsBatch),
      writeConcern.value ? { writeConcern: writeConcern.value } : undefined,
    )
    ctx.sendResult(id, { results: results.map(toExecuteResponse) })
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}

export async function handleLoadMessage(
  ctx: WSOperationContext,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (typeof msg.sql !== 'string') {
    ctx.sendError(id, 'INVALID_MESSAGE', 'Load message requires a "sql" string field')
    return
  }

  const paramsBatchError = paramsBatchValidationError(msg.paramsBatch)
  if (paramsBatchError !== null) {
    ctx.sendError(id, 'INVALID_MESSAGE', paramsBatchError)
    return
  }

  const durabilityError = loadDurabilityValidationError(msg.durability)
  if (durabilityError !== null) {
    ctx.sendError(id, 'INVALID_MESSAGE', durabilityError)
    return
  }

  const bulkLoad = ctx.target.bulkLoad
  if (typeof bulkLoad !== 'function') {
    ctx.sendError(id, 'BULK_LOAD_UNSUPPORTED', 'The execution target for this database does not support bulk load')
    return
  }

  const sql = msg.sql
  const paramsBatch = decodeBatchParams(ctx, id, msg.paramsBatch as (Record<string, unknown> | unknown[])[])
  if (paramsBatch === null) return
  const durability = msg.durability as 'off' | 'normal' | undefined

  try {
    const summary = await bulkLoad.call(ctx.target, sql, paramsBatch, durability ? { durability } : undefined)
    ctx.sendResult(id, summary)
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}
