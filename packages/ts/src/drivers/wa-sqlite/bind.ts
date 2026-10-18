import { bindableParams, isNamedParams } from '../../core/driver/values.js'

const SQLITE_OK = 0
const SQLITE_TRANSIENT = -1
const SMALLEST_INT64 = -(2n ** 63n)
const LARGEST_INT64 = 2n ** 63n - 1n
const SMALLEST_ALLOCATION_BYTES = 1

const utf8 = new TextEncoder()

type BindBytesCall = (stmt: number, index: number, pointer: number, bytes: number, destructor: number) => number

/**
 * The calls of the wa-sqlite API that {@link bindStatement} uses.
 *
 * @internal
 */
export interface WaSqliteBindings {
  bind_parameter_count(stmt: number): number
  bind_parameter_name(stmt: number, index: number): string | null
  bind_null(stmt: number, index: number): number
  bind_double(stmt: number, index: number, value: number): number
  bind_int64(stmt: number, index: number, value: bigint): number
}

/**
 * The exports of the wa-sqlite WebAssembly module that the driver calls to pass text and BLOB values in and out by
 * their length, because the wa-sqlite API ends text at its first NUL character, binds an empty BLOB as NULL, and
 * returns each BLOB as a view of memory that SQLite reuses on the next step.
 *
 * @internal
 */
export interface WaSqliteMemory {
  readonly HEAPU8: Uint8Array
  _sqlite3_malloc(bytes: number): number
  _sqlite3_free(pointer: number): void
  _sqlite3_bind_blob: BindBytesCall
  _sqlite3_bind_text: BindBytesCall
  _sqlite3_column_blob(stmt: number, index: number): number
  _sqlite3_column_text(stmt: number, index: number): number
  _sqlite3_column_bytes(stmt: number, index: number): number
}

function describeValue(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value !== 'object') return typeof value
  return Object.getPrototypeOf(value)?.constructor?.name ?? 'object'
}

function checked(rc: number, call: string): void {
  if (rc !== SQLITE_OK) throw new Error(`${call} failed with SQLite result code ${rc}`)
}

function bindBytes(
  memory: WaSqliteMemory,
  bind: BindBytesCall,
  call: string,
  stmt: number,
  index: number,
  bytes: Uint8Array,
): void {
  const pointer = memory._sqlite3_malloc(Math.max(bytes.byteLength, SMALLEST_ALLOCATION_BYTES))
  if (pointer === 0) throw new Error(`SQLite could not allocate ${bytes.byteLength} bytes to bind parameter ${index}`)
  try {
    memory.HEAPU8.set(bytes, pointer)
    checked(bind(stmt, index, pointer, bytes.byteLength, SQLITE_TRANSIENT), call)
  } finally {
    memory._sqlite3_free(pointer)
  }
}

function bindValue(
  sqlite3: WaSqliteBindings,
  memory: WaSqliteMemory,
  stmt: number,
  index: number,
  value: unknown,
): void {
  if (value === null) {
    checked(sqlite3.bind_null(stmt, index), 'sqlite3_bind_null')
    return
  }
  if (typeof value === 'number') {
    checked(sqlite3.bind_double(stmt, index, value), 'sqlite3_bind_double')
    return
  }
  if (typeof value === 'string') {
    bindBytes(memory, memory._sqlite3_bind_text, 'sqlite3_bind_text', stmt, index, utf8.encode(value))
    return
  }
  if (typeof value === 'bigint') {
    if (value < SMALLEST_INT64 || value > LARGEST_INT64) {
      throw new RangeError(`The integer ${value} lies outside the 64-bit range that SQLite stores`)
    }
    checked(sqlite3.bind_int64(stmt, index, value), 'sqlite3_bind_int64')
    return
  }
  if (ArrayBuffer.isView(value)) {
    const bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    bindBytes(memory, memory._sqlite3_bind_blob, 'sqlite3_bind_blob', stmt, index, bytes)
    return
  }
  throw new TypeError(`SQLite cannot bind a value of type ${describeValue(value)} to parameter ${index}`)
}

function bindNamed(
  sqlite3: WaSqliteBindings,
  memory: WaSqliteMemory,
  stmt: number,
  params: Record<string, unknown>,
): void {
  const count = sqlite3.bind_parameter_count(stmt)
  for (let index = 1; index <= count; index++) {
    const name = sqlite3.bind_parameter_name(stmt, index)
    if (!name) continue
    const value = params[name.slice(1)]
    if (value !== undefined) bindValue(sqlite3, memory, stmt, index, value)
  }
}

function bindPositional(
  sqlite3: WaSqliteBindings,
  memory: WaSqliteMemory,
  stmt: number,
  params: readonly unknown[],
): void {
  const count = sqlite3.bind_parameter_count(stmt)
  if (params.length > count) {
    throw new RangeError(`The statement takes ${count} parameter values, and the call provided ${params.length}`)
  }
  for (let index = 0; index < params.length; index++) {
    const value = params[index]
    if (value !== undefined) bindValue(sqlite3, memory, stmt, index + 1, value)
  }
}

/**
 * Binds the caller's parameters to a prepared wa-sqlite statement, positionally or by name, and refuses any value that
 * SQLite cannot store, so that no value that the caller passed becomes NULL.
 *
 * @param sqlite3 - The wa-sqlite API.
 * @param memory - The WebAssembly module that the API runs on.
 * @param stmt - The prepared statement.
 * @param params - The positional values, or a single object of named values.
 * @throws A `RangeError` for more positional values than the statement takes or for an integer outside the 64-bit
 * range, and a `TypeError` for a value of a type that SQLite cannot store.
 *
 * @internal
 */
export function bindStatement(
  sqlite3: WaSqliteBindings,
  memory: WaSqliteMemory,
  stmt: number,
  params: readonly unknown[],
): void {
  const bound = bindableParams(params)
  const [only] = bound
  if (bound.length === 1 && isNamedParams(only)) {
    bindNamed(sqlite3, memory, stmt, only)
    return
  }
  bindPositional(sqlite3, memory, stmt, bound)
}
