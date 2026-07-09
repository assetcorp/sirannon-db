import type { ServerExecutionTarget } from '../core/types.js'
import type { WSResultMessage } from './protocol.js'
import {
  loadDurabilityValidationError,
  paramsBatchValidationError,
  toExecuteResponse,
  transactionStatementsValidationError,
  validateWriteConcern,
} from './protocol.js'

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

  try {
    const params = (msg.params ?? undefined) as Record<string, unknown> | unknown[] | undefined
    const rows = await ctx.target.query(msg.sql, params)
    ctx.sendResult(id, { rows })
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

  try {
    const params = (msg.params ?? undefined) as Record<string, unknown> | unknown[] | undefined
    const result = await ctx.target.execute(msg.sql, params)
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

  try {
    const results = await ctx.target.transaction(
      async tx => {
        const txResults = []
        for (const stmt of validStatements) {
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
  const paramsBatch = msg.paramsBatch as (Record<string, unknown> | unknown[])[]

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
  const paramsBatch = msg.paramsBatch as (Record<string, unknown> | unknown[])[]
  const durability = msg.durability as 'off' | 'normal' | undefined

  try {
    const summary = await bulkLoad.call(ctx.target, sql, paramsBatch, durability ? { durability } : undefined)
    ctx.sendResult(id, summary)
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}
