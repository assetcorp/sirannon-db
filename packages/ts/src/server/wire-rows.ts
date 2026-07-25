import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { Params, QueryOptions, ServerExecutionTarget } from '../core/types.js'

export async function queryWireRows(
  target: ServerExecutionTarget,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): Promise<unknown[]> {
  if (target.queryForWire) {
    return target.queryForWire(sql, params, options)
  }
  return encodeTaggedValues(await target.query(sql, params, options)) as unknown[]
}
