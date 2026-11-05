import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { Params, QueryOptions, ServerExecutionTarget } from '../core/types.js'

/**
 * Returns the rows of a read encoded for a network response, using the target's own wire read when it has one and
 * encoding the rows of a plain query otherwise.
 *
 * @param target - The database or operation context that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern for this statement.
 * @returns The rows with every value encoded for the wire.
 * @internal
 */
export function queryWireRows(
  target: ServerExecutionTarget,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  if (target.queryForWire) {
    return target.queryForWire(sql, params, options)
  }
  return queryAndEncode(target, sql, params, options)
}

async function queryAndEncode(
  target: ServerExecutionTarget,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  return encodeTaggedValues(await target.query(sql, params, options)) as unknown[]
}
