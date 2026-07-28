import { useRef } from 'react'

export function useStableValue<T>(value: T): T {
  const held = useRef(value)
  if (held.current !== value && !sameValue(held.current, value)) held.current = value
  return held.current
}

export function sameValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (typeof left !== 'object' || typeof right !== 'object' || left === null || right === null) return false
  if (Array.isArray(left) || Array.isArray(right)) return sameArray(left, right)
  if (ArrayBuffer.isView(left) || ArrayBuffer.isView(right)) return sameBytes(left, right)
  if (left instanceof Date || right instanceof Date) {
    return left instanceof Date && right instanceof Date && left.getTime() === right.getTime()
  }
  return sameRecord(left as Record<string, unknown>, right as Record<string, unknown>)
}

function sameArray(left: unknown, right: unknown): boolean {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false
  for (let index = 0; index < left.length; index++) {
    if (!sameValue(left[index], right[index])) return false
  }
  return true
}

function sameBytes(left: unknown, right: unknown): boolean {
  if (!(left instanceof Uint8Array) || !(right instanceof Uint8Array) || left.length !== right.length) return false
  for (let index = 0; index < left.length; index++) {
    if (left[index] !== right[index]) return false
  }
  return true
}

function sameRecord(left: Record<string, unknown>, right: Record<string, unknown>): boolean {
  const keys = Object.keys(left)
  if (keys.length !== Object.keys(right).length) return false
  for (const key of keys) {
    if (!Object.hasOwn(right, key)) return false
    if (!sameValue(left[key], right[key])) return false
  }
  return true
}
