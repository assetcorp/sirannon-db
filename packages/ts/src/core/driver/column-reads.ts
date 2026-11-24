import { narrowArrayIntegers } from './values.js'

/**
 * The bytes that every value counts toward `maxBytes`, whatever its type. A TEXT or BLOB value counts its length on
 * top, so that a result of many NULL or empty values still reaches the limit.
 *
 * @internal
 */
export const BYTES_COUNTED_FOR_EVERY_VALUE = 8

/**
 * Describes one column of a result, in the order that the statement returns its columns.
 *
 * @public
 */
export interface ColumnDetail {
  /** The column's name in the result, which `AS` sets and which can repeat across columns. */
  name: string
  /** The type that the table's schema declares for the source column, or `null` for an expression or when the driver cannot report it. */
  declaredType: string | null
  /** The table that the value comes from, or `null` for an expression or when the driver cannot report it. */
  table: string | null
  /** The column of that table that the value comes from, or `null` for an expression or when the driver cannot report it. */
  column: string | null
  /** The schema that holds that table, such as `main`, or `null` for an expression or when the driver cannot report it. */
  database: string | null
}

/**
 * Names the limit that stopped a read before it returned every row: `rows` for `maxRows` and `bytes` for `maxBytes`.
 *
 * @public
 */
export type ReadTruncation = 'rows' | 'bytes'

/**
 * The limits that a driver applies while it reads the rows of {@link SQLiteStatement.allWithColumns}.
 *
 * @public
 */
export interface ColumnReadLimits {
  /** The most rows that the read returns. The driver reads one row past it to learn whether more rows exist. */
  maxRows?: number
  /**
   * The most bytes of values that the read returns. Every value counts 8 bytes, a TEXT value also counts the bytes of
   * its UTF-8 encoding, and a BLOB also counts its length. The driver returns no row that would take the total past
   * this limit.
   */
  maxBytes?: number
  /** `true` to leave every integer as a `bigint`, for a caller that encodes the rows for the wire in a separate pass. */
  rawIntegers?: boolean
}

/**
 * What {@link SQLiteStatement.allWithColumns} returns: the columns, the rows as arrays, and the limit that stopped the read.
 *
 * @public
 */
export interface ColumnRead {
  /** One entry for each result column, in order. */
  columns: ColumnDetail[]
  /** One array for each row, holding the row's values in column order. */
  rows: unknown[][]
  /** The limit that stopped the read, or `null` when the read returned every row. */
  truncated: ReadTruncation | null
  /** The number of rows that SQLite read during the call, which a driver reporting `rowCounts` sets. */
  rowsRead?: number
  /** The number of rows that SQLite wrote during the call, which a driver reporting `rowCounts` sets. */
  rowsWritten?: number
}

/**
 * Returns the number of bytes in the UTF-8 encoding of a string, counting an unpaired surrogate as the three bytes of
 * the replacement character that an encoder writes in its place.
 *
 * @param text - The string.
 * @returns The length of its UTF-8 encoding in bytes.
 * @internal
 */
export function utf8ByteLength(text: string): number {
  const length = text.length
  let bytes = length
  for (let index = 0; index < length; index++) {
    const code = text.charCodeAt(index)
    if (code < 0x80) continue
    if (code < 0x800) {
      bytes += 1
      continue
    }
    if (code >= 0xd800 && code <= 0xdbff && index + 1 < length) {
      const next = text.charCodeAt(index + 1)
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 2
        index++
        continue
      }
    }
    bytes += 2
  }
  return bytes
}

function valueBytes(value: unknown): number {
  if (typeof value === 'string') return BYTES_COUNTED_FOR_EVERY_VALUE + utf8ByteLength(value)
  if (value instanceof Uint8Array) return BYTES_COUNTED_FOR_EVERY_VALUE + value.byteLength
  return BYTES_COUNTED_FOR_EVERY_VALUE
}

/**
 * Returns the bytes that one row counts toward `maxBytes`, or stops counting once the total passes `budget`. Every
 * value counts {@link BYTES_COUNTED_FOR_EVERY_VALUE} bytes, a TEXT value also counts the bytes of its UTF-8 encoding,
 * and a BLOB also counts its length.
 *
 * @param row - The row's values.
 * @param budget - The bytes that the read can still return.
 * @returns The row's bytes, or a number above `budget` once the row cannot fit.
 * @internal
 */
export function rowBytesWithin(row: readonly unknown[], budget: number): number {
  let total = 0
  for (let index = 0; index < row.length; index++) {
    total += valueBytes(row[index])
    if (total > budget) return total
  }
  return total
}

/**
 * Returns whether a read carries a row or byte limit, so that a driver can read every row in one call when it carries none.
 *
 * @param limits - The limits of the read.
 * @returns `true` when `maxRows` or `maxBytes` is set.
 * @internal
 */
export function limitsRows(limits: ColumnReadLimits): boolean {
  return limits.maxRows !== undefined || limits.maxBytes !== undefined
}

/**
 * Gathers the rows of one read as a driver steps through them, and stops the read at its row and byte limits.
 *
 * @internal
 */
export class RowCollector {
  /** The rows that the read returns, in order. */
  readonly rows: unknown[][] = []
  /** The limit that stopped the read, or `null` while every row fits. */
  truncated: ReadTruncation | null = null
  private bytes = 0

  constructor(private readonly limits: ColumnReadLimits) {}

  /**
   * Takes the next row that SQLite returned, and narrows its integers unless the read keeps them raw.
   *
   * @param row - The row's values in column order, which the collector keeps and may change.
   * @returns `true` while the driver should read another row, and `false` once a limit stops the read.
   */
  accept(row: unknown[]): boolean {
    const { maxRows, maxBytes } = this.limits
    if (maxRows !== undefined && this.rows.length >= maxRows) {
      this.truncated = 'rows'
      return false
    }
    if (maxBytes !== undefined) {
      const total = this.bytes + rowBytesWithin(row, maxBytes - this.bytes)
      if (total > maxBytes) {
        this.truncated = 'bytes'
        return false
      }
      this.bytes = total
    }
    this.rows.push(this.limits.rawIntegers === true ? row : narrowArrayIntegers(row))
    return true
  }
}

/**
 * Returns rows that a driver read in one call for a read without limits, with their integers narrowed unless the
 * read keeps them raw.
 *
 * @param rows - Every row of the result, which this function may change.
 * @param limits - The limits of the read, which set no row or byte limit.
 * @returns The same rows.
 * @internal
 */
export function unlimitedRows(rows: unknown[][], limits: ColumnReadLimits): unknown[][] {
  if (limits.rawIntegers !== true) for (const row of rows) narrowArrayIntegers(row)
  return rows
}
