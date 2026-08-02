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
