const SAFE_INT_BOUND = 9007199254740991n

export function narrowSafeBigInt(value: unknown): unknown {
  if (typeof value === 'bigint' && value >= -SAFE_INT_BOUND && value <= SAFE_INT_BOUND) {
    return Number(value)
  }
  return value
}

export function narrowRowIntegers<T>(row: T): T {
  if (row === null || typeof row !== 'object') return row
  const obj = row as Record<string, unknown>
  for (const key of Object.keys(obj)) {
    const value = obj[key]
    if (typeof value === 'bigint' && value >= -SAFE_INT_BOUND && value <= SAFE_INT_BOUND) {
      obj[key] = Number(value)
    }
  }
  return row
}

export function narrowRowsIntegers<T>(rows: T[]): T[] {
  for (const row of rows) {
    narrowRowIntegers(row)
  }
  return rows
}

const EMPTY_BLOB_WITH_ALLOCATED_STORAGE = new Uint8Array(new ArrayBuffer(1), 0, 0)

function bindableValue(value: unknown): unknown {
  if (typeof value === 'boolean') return value ? 1n : 0n
  if (ArrayBuffer.isView(value) && value.byteLength === 0) return EMPTY_BLOB_WITH_ALLOCATED_STORAGE
  return value
}

/**
 * Returns whether a parameter value is a plain object of named values, which every driver binds by name.
 *
 * @param value - One value from the parameters that the caller passed.
 * @returns `true` for an object whose prototype is `Object.prototype` or `null`.
 *
 * @internal
 */
export function isNamedParams(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function bindableNamedParams(params: Record<string, unknown>): Record<string, unknown> {
  let bound: Record<string, unknown> | undefined
  for (const key of Object.keys(params)) {
    const value = params[key]
    const replacement = bindableValue(value)
    if (replacement === value) continue
    bound ??= { ...params }
    bound[key] = replacement
  }
  return bound ?? params
}

/**
 * Returns the parameters in a form that every SQLite engine binds the same way, so that SQLite stores a boolean as
 * the integer 1 or 0 and stores an empty byte array as an empty BLOB.
 *
 * @param params - The positional values, or a single object of named values, that the caller passed.
 * @returns `params` itself when no value needs a change, and otherwise a copy holding the changed values.
 *
 * @internal
 */
export function bindableParams(params: readonly unknown[]): unknown[] {
  let bound: unknown[] | undefined
  for (let index = 0; index < params.length; index++) {
    const value = params[index]
    const replacement = isNamedParams(value) ? bindableNamedParams(value) : bindableValue(value)
    if (replacement === value) continue
    bound ??= [...params]
    bound[index] = replacement
  }
  return bound ?? (params as unknown[])
}
