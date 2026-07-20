const SAFE_INT_BOUND = 9007199254740991n

/**
 * Drivers read SQLite integers as BigInt so 64-bit values cross the JS
 * boundary without rounding; narrowing values inside the safe range back to
 * numbers keeps the package convention that only integers beyond 2^53 - 1
 * surface as BigInt.
 */
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
