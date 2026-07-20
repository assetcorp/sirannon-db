import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { Params, QueryOptions, ServerExecutionTarget } from '../core/types.js'

/**
 * Resolves query result rows in their wire form. Prefers the target's
 * single-pass {@link ServerExecutionTarget.queryForWire}; a target without it
 * (a custom or wrapping target) still returns correct output by encoding the
 * native {@link ServerExecutionTarget.query} rows here.
 */
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
