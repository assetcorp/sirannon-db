import { narrowSafeBigInt } from '../../core/driver/values.js'
import type { WaSqliteMemory } from './bind.js'

const SQLITE_INTEGER = 1
const SQLITE_FLOAT = 2
const SQLITE_TEXT = 3
const SQLITE_BLOB = 4

const utf8 = new TextDecoder('utf-8', { ignoreBOM: true })

/**
 * The calls of the wa-sqlite API that {@link readColumn} uses.
 *
 * @internal
 */
export interface WaSqliteColumns {
  column_type(stmt: number, index: number): number
  column_int64(stmt: number, index: number): bigint
  column_double(stmt: number, index: number): number
}

/**
 * Returns the value in one column of the current row, so that an integer reads as a number within the safe range and
 * as a bigint beyond it, text reads in full past any NUL character, and a BLOB reads as a copy that outlives the step.
 *
 * @param sqlite3 - The wa-sqlite API.
 * @param memory - The WebAssembly module that the API runs on.
 * @param stmt - The statement, stepped onto a row.
 * @param index - The zero-based column index.
 * @returns The value, or `null` for a NULL column.
 *
 * @internal
 */
export function readColumn(sqlite3: WaSqliteColumns, memory: WaSqliteMemory, stmt: number, index: number): unknown {
  switch (sqlite3.column_type(stmt, index)) {
    case SQLITE_INTEGER:
      return narrowSafeBigInt(sqlite3.column_int64(stmt, index))
    case SQLITE_FLOAT:
      return sqlite3.column_double(stmt, index)
    case SQLITE_TEXT: {
      const pointer = memory._sqlite3_column_text(stmt, index)
      const length = memory._sqlite3_column_bytes(stmt, index)
      return utf8.decode(memory.HEAPU8.subarray(pointer, pointer + length))
    }
    case SQLITE_BLOB: {
      const pointer = memory._sqlite3_column_blob(stmt, index)
      const length = memory._sqlite3_column_bytes(stmt, index)
      return memory.HEAPU8.slice(pointer, pointer + length)
    }
    default:
      return null
  }
}
