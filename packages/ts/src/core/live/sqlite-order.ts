import { CDCError } from '../errors.js'
import type { SortTerm } from './statement-shape.js'

export type Collation = 'binary' | 'nocase' | 'rtrim'

const TYPE_NULL = 0
const TYPE_NUMBER = 1
const TYPE_TEXT = 2
const TYPE_BLOB = 3

export type SortValue =
  | { type: typeof TYPE_NULL }
  | { type: typeof TYPE_NUMBER; numeric: number | bigint }
  | { type: typeof TYPE_TEXT | typeof TYPE_BLOB; bytes: Uint8Array }

const encoder = new TextEncoder()

export function resolveCollation(name: string | null): Collation {
  if (name === null) return 'binary'
  const lower = name.toLowerCase()
  if (lower === 'binary' || lower === 'nocase' || lower === 'rtrim') return lower
  throw new CDCError(
    `Cannot keep a live query ordered by the '${name}' collation: order is maintained for BINARY, NOCASE, and RTRIM`,
  )
}

export function toSortValue(value: unknown, collation: Collation): SortValue {
  if (value === null || value === undefined) return { type: TYPE_NULL }
  if (typeof value === 'number' || typeof value === 'bigint') return { type: TYPE_NUMBER, numeric: value }
  if (typeof value === 'boolean') return { type: TYPE_NUMBER, numeric: value ? 1 : 0 }
  if (typeof value === 'string') return { type: TYPE_TEXT, bytes: encoder.encode(applyCollation(value, collation)) }
  if (value instanceof Uint8Array) return { type: TYPE_BLOB, bytes: value }
  if (ArrayBuffer.isView(value)) {
    const view = value as ArrayBufferView
    return { type: TYPE_BLOB, bytes: new Uint8Array(view.buffer, view.byteOffset, view.byteLength) }
  }
  return { type: TYPE_TEXT, bytes: encoder.encode(applyCollation(String(value), collation)) }
}

function applyCollation(text: string, collation: Collation): string {
  if (collation === 'nocase') return upperAscii(text)
  if (collation === 'rtrim') return trimTrailingSpaces(text)
  return text
}

function upperAscii(text: string): string {
  let result = ''
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i)
    result += code >= 97 && code <= 122 ? String.fromCharCode(code - 32) : text[i]
  }
  return result
}

function trimTrailingSpaces(text: string): string {
  let end = text.length
  while (end > 0 && text.charCodeAt(end - 1) === 32) end--
  return text.slice(0, end)
}

export function compareSortValues(left: SortValue, right: SortValue): number {
  if (left.type !== right.type) return left.type < right.type ? -1 : 1
  if (left.type === TYPE_NULL) return 0
  if (left.type === TYPE_NUMBER) return compareNumeric(left.numeric, (right as { numeric: number | bigint }).numeric)
  return compareBytes(left.bytes, (right as { bytes: Uint8Array }).bytes)
}

function compareNumeric(left: number | bigint, right: number | bigint): number {
  if (typeof left === 'number' && typeof right === 'number') {
    return left < right ? -1 : left > right ? 1 : 0
  }
  if (typeof left === 'bigint' && typeof right === 'bigint') {
    return left < right ? -1 : left > right ? 1 : 0
  }
  const big = typeof left === 'bigint' ? left : (right as bigint)
  const real = typeof left === 'bigint' ? (right as number) : left
  const sign = typeof left === 'bigint' ? 1 : -1

  if (!Number.isFinite(real)) return real > 0 ? -sign : sign
  const whole = Math.floor(real)
  const wholeAsBig = BigInt(whole)
  if (big !== wholeAsBig) return big < wholeAsBig ? -sign : sign
  return real > whole ? -sign : 0
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const shared = Math.min(left.length, right.length)
  for (let i = 0; i < shared; i++) {
    if (left[i] !== right[i]) return left[i] < right[i] ? -1 : 1
  }
  return left.length === right.length ? 0 : left.length < right.length ? -1 : 1
}

export interface SortKeyPlan {
  collations: Collation[]
  compare(left: readonly SortValue[], right: readonly SortValue[]): number
}

export function buildSortKeyPlan(terms: readonly SortTerm[]): SortKeyPlan {
  const collations = terms.map(term => resolveCollation(term.collation))
  const descending = terms.map(term => term.direction === 'desc')
  const nullsLast = terms.map(term => term.nulls === 'last')

  return {
    collations,
    compare(left, right) {
      for (let i = 0; i < terms.length; i++) {
        const a = left[i]
        const b = right[i]
        const aNull = a.type === TYPE_NULL
        const bNull = b.type === TYPE_NULL
        if (aNull !== bNull) return (aNull ? 1 : -1) * (nullsLast[i] ? 1 : -1)
        if (aNull) continue
        const order = compareSortValues(a, b)
        if (order !== 0) return descending[i] ? -order : order
      }
      return 0
    },
  }
}

export function lowerBoundIndex(
  rows: readonly { sort: readonly SortValue[] }[],
  key: readonly SortValue[],
  plan: SortKeyPlan,
): number {
  let low = 0
  let high = rows.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if (plan.compare(rows[middle].sort, key) < 0) low = middle + 1
    else high = middle
  }
  return low
}

export function placementIndex(
  rows: readonly { sort: readonly SortValue[] }[],
  key: readonly SortValue[],
  plan: SortKeyPlan,
): number {
  let low = 0
  let high = rows.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if (plan.compare(rows[middle].sort, key) <= 0) low = middle + 1
    else high = middle
  }
  return low
}
