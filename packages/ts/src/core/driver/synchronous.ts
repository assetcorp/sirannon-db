import { SirannonError } from '../errors.js'
import type { SynchronousLevel } from './types.js'

/**
 * The writer durability level that Sirannon applies when you pass no `synchronous` option.
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
 * Returns the `PRAGMA synchronous` argument for a durability level from a fixed
 * lookup table, so that the SQL string can contain only `OFF`, `NORMAL`,
 * `FULL`, or `EXTRA`.
 *
 * @param level - The durability level, or `undefined` for {@link DEFAULT_SYNCHRONOUS}.
 * @returns The upper-case `PRAGMA synchronous` argument.
 * @throws {@link SirannonError} with code `INVALID_SYNCHRONOUS` when `level` is none of the four levels.
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
 * Returns `true` when a value is one of the durability levels that SQLite accepts for `PRAGMA synchronous`.
 *
 * @param value - The value to check.
 * @returns `true` when the value is `off`, `normal`, `full`, or `extra`.
 *
 * @public
 */
export function isSynchronousLevel(value: unknown): value is SynchronousLevel {
  return value === 'off' || value === 'normal' || value === 'full' || value === 'extra'
}
