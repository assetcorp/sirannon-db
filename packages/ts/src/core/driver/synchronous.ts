import { SirannonError } from '../errors.js'
import type { SynchronousLevel } from './types.js'

/**
 * Writer durability a database opens at when you name none.
 *
 * @public
 */
export const DEFAULT_SYNCHRONOUS: SynchronousLevel = 'normal'

const SYNCHRONOUS_PRAGMA_VALUES: Record<SynchronousLevel, string> = {
  off: 'OFF',
  normal: 'NORMAL',
  full: 'FULL',
  extra: 'EXTRA',
}

/**
 * Map a synchronous level to its PRAGMA argument through an allowlist so that a
 * value smuggled past the type system can never reach SQL interpolation.
 *
 * @public
 */
export function synchronousPragmaValue(level: SynchronousLevel | undefined): string {
  const resolved = level ?? DEFAULT_SYNCHRONOUS
  const value = SYNCHRONOUS_PRAGMA_VALUES[resolved]
  if (value === undefined) {
    throw new SirannonError(
      `Invalid synchronous level '${String(resolved)}': expected 'off', 'normal', 'full', or 'extra'`,
      'INVALID_SYNCHRONOUS',
    )
  }
  return value
}

/**
 * Reports whether a value is one of the durability levels SQLite accepts.
 *
 * @param value - The value to check.
 * @returns True when it is a valid level.
 *
 * @public
 */
export function isSynchronousLevel(value: unknown): value is SynchronousLevel {
  return value === 'off' || value === 'normal' || value === 'full' || value === 'extra'
}
