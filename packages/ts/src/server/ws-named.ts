import type { OperationArguments } from '../core/operation-registry.js'
import type { OperationSource } from './operation-lookup.js'
import { decodeBoundParams, toExecuteResponse, validateReadConcern, validateWriteConcern } from './protocol.js'
import { queryWireRows } from './wire-rows.js'
import type { WSOperationContext } from './ws-operations.js'
import { toReadOptions } from './ws-operations.js'

export interface WSNamedContext extends WSOperationContext {
  databaseId: string
  identity: unknown
  operations: OperationSource
}

export type ArgumentsRead = { ok: true; value: OperationArguments } | { ok: false; message: string }

export function readArguments(raw: unknown): ArgumentsRead {
  if (raw === undefined) return { ok: true, value: {} }
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return { ok: false, message: '"args" must be an object' }
  }
  const decoded = decodeBoundParams(raw, 'args')
  if (!decoded.ok) return { ok: false, message: decoded.message }
  return { ok: true, value: (decoded.value ?? {}) as OperationArguments }
}

export async function handleNamedQueryMessage(
  ctx: WSNamedContext,
  msg: Record<string, unknown>,
  id: string,
  name: string,
): Promise<void> {
  const args = readArguments(msg.args)
  if (!args.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', args.message)
    return
  }

  const readConcern = validateReadConcern(msg.readConcern)
  if (!readConcern.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', readConcern.message)
    return
  }

  try {
    const resolved = ctx.operations.resolve('read', ctx.databaseId, name, args.value, ctx.identity)
    if (!resolved.ok) {
      ctx.sendError(id, resolved.refusal.code, resolved.refusal.message)
      return
    }
    const statement = resolved.statements[0]
    const rows = await queryWireRows(ctx.target, statement.sql, statement.params, toReadOptions(readConcern.value))
    ctx.sendResult(id, { rows: rows as Record<string, unknown>[] })
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}

export async function handleNamedExecuteMessage(
  ctx: WSNamedContext,
  msg: Record<string, unknown>,
  id: string,
  name: string,
): Promise<void> {
  const args = readArguments(msg.args)
  if (!args.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', args.message)
    return
  }

  const writeConcern = validateWriteConcern(msg.writeConcern)
  if (!writeConcern.ok) {
    ctx.sendError(id, 'INVALID_MESSAGE', writeConcern.message)
    return
  }

  try {
    const resolved = ctx.operations.resolve('write', ctx.databaseId, name, args.value, ctx.identity)
    if (!resolved.ok) {
      ctx.sendError(id, resolved.refusal.code, resolved.refusal.message)
      return
    }

    const statements = resolved.statements
    const options = writeConcern.value ? { writeConcern: writeConcern.value } : undefined
    const results = ctx.target.executeTransaction
      ? await ctx.target.executeTransaction(statements, options)
      : await ctx.target.transaction(async tx => {
          const collected = []
          for (const statement of statements) {
            collected.push(await tx.execute(statement.sql, statement.params))
          }
          return collected
        }, options)
    ctx.sendResult(id, { results: results.map(toExecuteResponse) })
  } catch (err) {
    ctx.sendCaughtError(id, err)
  }
}
