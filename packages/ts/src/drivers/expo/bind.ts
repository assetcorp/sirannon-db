import type { SQLiteBindParams, SQLiteBindValue } from 'expo-sqlite'
import { isNamedParams } from '../../core/driver/values.js'

const LARGEST_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER)
const NAMED_PARAMETER_PREFIXES = [':', '@', '$'] as const

function expoValue(value: unknown): SQLiteBindValue {
  if (typeof value !== 'bigint') return value as SQLiteBindValue
  if (value < -LARGEST_SAFE_INTEGER || value > LARGEST_SAFE_INTEGER) {
    throw new RangeError(
      `expo-sqlite passes every number to SQLite as a JavaScript number, which holds no integer beyond ±${Number.MAX_SAFE_INTEGER}, so it cannot bind ${value}`,
    )
  }
  return Number(value)
}

function expoNamedParams(params: Record<string, unknown>): Record<string, SQLiteBindValue> {
  const named: Record<string, SQLiteBindValue> = {}
  for (const key of Object.keys(params)) {
    const value = params[key]
    if (value === undefined) continue
    const bound = expoValue(value)
    named[key] = bound
    for (const prefix of NAMED_PARAMETER_PREFIXES) named[`${prefix}${key}`] = bound
  }
  return named
}

/**
 * Returns the parameters in the form that `expo-sqlite` binds, so that a single object of named values binds by name
 * under each prefix that SQLite accepts and a bigint within the safe range binds as the same integer.
 *
 * @param params - The positional values, or a single object of named values, that the caller passed.
 * @returns The positional values as an array, or the named values keyed by each prefixed parameter name.
 * @throws A `RangeError` for a bigint beyond the safe integer range, which `expo-sqlite` has no way to pass.
 *
 * @internal
 */
export function expoBindParams(params: readonly unknown[]): SQLiteBindParams {
  const [only] = params
  if (params.length === 1 && isNamedParams(only)) return expoNamedParams(only)
  return params.map(expoValue)
}
