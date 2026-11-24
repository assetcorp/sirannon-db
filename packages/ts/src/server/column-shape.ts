import { encodeTaggedValues } from '../core/cdc/encoding.js'
import { SirannonError } from '../core/errors.js'
import type {
  Params,
  QueryWithColumnsOptions,
  QueryWithColumnsResult,
  ReadConcern,
  ServerExecutionTarget,
} from '../core/types.js'

const COLUMNS_SHAPE = 'columns'
const TARGET_READS_NO_COLUMNS_MESSAGE =
  "The server's execution target for this database reads no column details, so it cannot answer shape 'columns'"

/**
 * The shape that a read request asks for: today's rows, or columns with rows as arrays under two limits.
 *
 * @internal
 */
export type RequestedReadShape =
  | { ok: true; columns: false }
  | { ok: true; columns: true; maxRows?: number; maxBytes?: number }
  | { ok: false; message: string }

function limitError(name: 'maxRows' | 'maxBytes', value: unknown): string | null {
  if (value === undefined) return null
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return null
  return `"${name}" must be a non-negative safe integer`
}

/**
 * Returns the shape that the body of a read request asks for, after checking `shape`, `maxRows`, and `maxBytes`.
 *
 * @param body - The parsed request body or WebSocket message.
 * @returns The rows shape when `shape` is absent, the columns shape with its limits when `shape` is `columns`, and a
 * refusal for any other `shape`, for a limit that is not a non-negative safe integer, or for a limit without
 * `shape`.
 * @internal
 */
export function requestedReadShape(body: Record<string, unknown>): RequestedReadShape {
  const { shape, maxRows, maxBytes } = body
  if (shape === undefined) {
    if (maxRows !== undefined || maxBytes !== undefined) {
      return { ok: false, message: '"maxRows" and "maxBytes" apply only when "shape" is "columns"' }
    }
    return { ok: true, columns: false }
  }
  if (shape !== COLUMNS_SHAPE) return { ok: false, message: '"shape" must be "columns" when it is present' }
  const message = limitError('maxRows', maxRows) ?? limitError('maxBytes', maxBytes)
  if (message !== null) return { ok: false, message }
  return {
    ok: true,
    columns: true,
    ...(maxRows === undefined ? {} : { maxRows: maxRows as number }),
    ...(maxBytes === undefined ? {} : { maxBytes: maxBytes as number }),
  }
}

/**
 * Returns the options of a columns read from the limits of a request and its read concern.
 *
 * @param shape - The columns shape that {@link requestedReadShape} returned.
 * @param readConcern - The read concern of the request, when it carries one.
 * @returns The options to pass to the execution target.
 * @internal
 */
export function columnReadOptions(
  shape: { maxRows?: number; maxBytes?: number },
  readConcern: ReadConcern | undefined,
): QueryWithColumnsOptions {
  return {
    ...(readConcern === undefined ? {} : { readConcern }),
    ...(shape.maxRows === undefined ? {} : { maxRows: shape.maxRows }),
    ...(shape.maxBytes === undefined ? {} : { maxBytes: shape.maxBytes }),
  }
}

/**
 * Returns the columns and rows of a read encoded for a network response, using the target's own wire read when it has
 * one and encoding the rows of its plain columns read otherwise.
 *
 * @param target - The database or proxy that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @param options - The read concern and the two limits.
 * @returns The columns, the encoded rows, and the limit that stopped the read.
 * @throws A `SirannonError` with `COLUMN_DETAILS_UNSUPPORTED` when the target reads no column details.
 * @internal
 */
export async function queryWireColumns(
  target: ServerExecutionTarget,
  sql: string,
  params: Params | undefined,
  options: QueryWithColumnsOptions,
): Promise<QueryWithColumnsResult> {
  if (target.queryWithColumnsForWire) return target.queryWithColumnsForWire(sql, params, options)
  if (!target.queryWithColumns) throw new SirannonError(TARGET_READS_NO_COLUMNS_MESSAGE, 'COLUMN_DETAILS_UNSUPPORTED')
  const result = await target.queryWithColumns(sql, params, options)
  return { columns: result.columns, rows: encodeTaggedValues(result.rows) as unknown[][], truncated: result.truncated }
}
