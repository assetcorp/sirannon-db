import { decodeTaggedValues, encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ColumnDetail, ReadTruncation } from '../core/driver/column-reads.js'
import type { Params, QueryWithColumnsOptions, ReadConcern } from '../core/types.js'
import type { QueryColumnsResponse } from '../server/protocol.js'
import { RemoteError } from './types.js'

const COLUMNS_SHAPE = 'columns'
const MAX_SQLITE_INTEGER = (1n << 63n) - 1n
const MIN_SQLITE_INTEGER = -(1n << 63n)
const COLUMN_DETAIL_FIELDS = ['declaredType', 'table', 'column', 'database'] as const
/**
 * The message of the `TRANSPORT_ERROR` that a columns read raises on a transport without a columns read.
 *
 * @internal
 */
export const COLUMN_READS_UNSUPPORTED_MESSAGE =
  'This transport sends no columns read; the HTTP and WebSocket transports of this package send one'
const MALFORMED_ANSWER_MESSAGE =
  'The server answered a columns read with a body that holds no valid columns, rows, and truncated fields'

/**
 * The read concern and the two limits that a transport sends with a columns read.
 *
 * @public
 */
export interface ColumnQueryRequest {
  /** The read concern that the server applies to the read. */
  readConcern?: ReadConcern
  /** The most rows that the answer holds. */
  maxRows?: number
  /** The most bytes of values that the answer holds. */
  maxBytes?: number
}

/**
 * The body of a columns read, which the HTTP route and the WebSocket `query` message share.
 *
 * @internal
 */
export interface ColumnQueryBody {
  sql: string
  params?: Params
  readConcern?: ReadConcern
  shape: 'columns'
  maxRows?: number
  maxBytes?: number
}

function validLimit(name: 'maxRows' | 'maxBytes', value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return value
  throw new RemoteError('INVALID_ARGUMENT', `${name} must be a non-negative safe integer`)
}

/**
 * Returns the request that a transport sends for a columns read, after checking the caller's limits.
 *
 * @param options - The caller's read concern and limits.
 * @returns The request.
 * @throws A `RemoteError` with `INVALID_ARGUMENT` when a limit is not a non-negative safe integer.
 * @internal
 */
export function columnQueryRequest(options: QueryWithColumnsOptions | undefined): ColumnQueryRequest {
  const maxRows = validLimit('maxRows', options?.maxRows)
  const maxBytes = validLimit('maxBytes', options?.maxBytes)
  return {
    ...(options?.readConcern === undefined ? {} : { readConcern: options.readConcern }),
    ...(maxRows === undefined ? {} : { maxRows }),
    ...(maxBytes === undefined ? {} : { maxBytes }),
  }
}

/**
 * Returns the body of a columns read, with its parameters in the tagged wire encoding.
 *
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement.
 * @param request - The read concern and the two limits.
 * @returns The body to send.
 * @internal
 */
export function columnQueryBody(
  sql: string,
  params: Params | undefined,
  request?: ColumnQueryRequest,
): ColumnQueryBody {
  return {
    sql,
    ...(params === undefined ? {} : { params: encodeTaggedValues(params) as Params }),
    ...(request?.readConcern === undefined ? {} : { readConcern: request.readConcern }),
    shape: COLUMNS_SHAPE,
    ...(request?.maxRows === undefined ? {} : { maxRows: request.maxRows }),
    ...(request?.maxBytes === undefined ? {} : { maxBytes: request.maxBytes }),
  }
}

function isColumnDetail(value: unknown): value is ColumnDetail {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false
  const column = value as Record<string, unknown>
  if (typeof column.name !== 'string') return false
  return COLUMN_DETAIL_FIELDS.every(field => column[field] === null || typeof column[field] === 'string')
}

function isTruncation(value: unknown): value is ReadTruncation | null {
  return value === null || value === 'rows' || value === 'bytes'
}

function decodedRow(row: unknown, width: number): unknown[] {
  if (!Array.isArray(row) || row.length !== width) throw new RemoteError('INVALID_RESPONSE', MALFORMED_ANSWER_MESSAGE)
  for (let index = 0; index < width; index++) {
    const cell = row[index]
    if (cell !== null && typeof cell === 'object') {
      const decoded = decodeTaggedValues(cell)
      if (typeof decoded === 'bigint' && (decoded < MIN_SQLITE_INTEGER || decoded > MAX_SQLITE_INTEGER)) {
        throw new RemoteError('INVALID_RESPONSE', MALFORMED_ANSWER_MESSAGE)
      }
      row[index] = decoded
    }
  }
  return row
}

/**
 * Returns the columns, rows, and truncation of a server's answer to a columns read, with each tagged value decoded.
 *
 * @param answer - The body that the server sent.
 * @returns The answer, whose rows this function decodes in place.
 * @throws A `RemoteError` with `INVALID_RESPONSE` when the body lacks the shape of a columns answer, when a row's
 * width differs from the number of columns, or when a tagged value is malformed.
 * @internal
 */
export function columnsAnswerFrom(answer: unknown): QueryColumnsResponse {
  if (typeof answer !== 'object' || answer === null) throw new RemoteError('INVALID_RESPONSE', MALFORMED_ANSWER_MESSAGE)
  const { columns, rows, truncated } = answer as Record<string, unknown>
  if (!Array.isArray(columns) || !columns.every(isColumnDetail) || !Array.isArray(rows) || !isTruncation(truncated)) {
    throw new RemoteError('INVALID_RESPONSE', MALFORMED_ANSWER_MESSAGE)
  }
  try {
    for (let index = 0; index < rows.length; index++) rows[index] = decodedRow(rows[index], columns.length)
  } catch (err) {
    if (err instanceof RemoteError) throw err
    throw new RemoteError('INVALID_RESPONSE', MALFORMED_ANSWER_MESSAGE)
  }
  const details = columns.map(column => ({
    name: column.name,
    declaredType: column.declaredType,
    table: column.table,
    column: column.column,
    database: column.database,
  }))
  return { columns: details, rows: rows as unknown[][], truncated }
}
