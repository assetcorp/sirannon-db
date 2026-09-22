import { encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { BulkLoadDurability, Params, ReadConcern, WriteConcern } from '../../core/types.js'
import type { WSClientMessage } from '../../server/protocol.js'

function encodedParams(params?: Params): Params | undefined {
  return encodeTaggedValues(params) as Params | undefined
}

function encodedArgs(args?: Record<string, unknown>): Record<string, unknown> | undefined {
  return args === undefined ? undefined : (encodeTaggedValues(args) as Record<string, unknown>)
}

export function queryFrame(id: string, sql: string, params?: Params, readConcern?: ReadConcern): WSClientMessage {
  return { type: 'query', id, sql, params: encodedParams(params), ...(readConcern ? { readConcern } : {}) }
}

export function executeFrame(id: string, sql: string, params?: Params): WSClientMessage {
  return { type: 'execute', id, sql, params: encodedParams(params) }
}

export function transactionFrame(id: string, statements: Array<{ sql: string; params?: Params }>): WSClientMessage {
  return {
    type: 'transaction',
    id,
    statements: statements.map(statement => ({ sql: statement.sql, params: encodedParams(statement.params) })),
  }
}

export function batchFrame(
  id: string,
  sql: string,
  paramsBatch: Params[],
  writeConcern?: WriteConcern,
): WSClientMessage {
  return {
    type: 'batch',
    id,
    sql,
    paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry) as Params),
    ...(writeConcern ? { writeConcern } : {}),
  }
}

export function loadFrame(
  id: string,
  sql: string,
  paramsBatch: Params[],
  durability?: BulkLoadDurability,
  checkpoint?: boolean,
): WSClientMessage {
  return {
    type: 'load',
    id,
    sql,
    paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry) as Params),
    ...(durability ? { durability } : {}),
    ...(checkpoint !== undefined ? { checkpoint } : {}),
  }
}

export function namedQueryFrame(
  id: string,
  name: string,
  args?: Record<string, unknown>,
  readConcern?: ReadConcern,
): WSClientMessage {
  const encoded = encodedArgs(args)
  return {
    type: 'query',
    id,
    name,
    ...(encoded === undefined ? {} : { args: encoded }),
    ...(readConcern ? { readConcern } : {}),
  }
}

export function namedExecuteFrame(
  id: string,
  name: string,
  args?: Record<string, unknown>,
  writeConcern?: WriteConcern,
): WSClientMessage {
  const encoded = encodedArgs(args)
  return {
    type: 'execute',
    id,
    name,
    ...(encoded === undefined ? {} : { args: encoded }),
    ...(writeConcern ? { writeConcern } : {}),
  }
}
