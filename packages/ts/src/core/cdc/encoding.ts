export const BLOB_TAG = '__sirannon_blob'
export const INT_TAG = '__sirannon_int'

export const SAFE_INT_BOUND_TEXT = '9007199254740991'

export function decodeTaggedValues(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value
  if (Array.isArray(value)) {
    return value.map(decodeTaggedValues)
  }
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1) {
    const tag = keys[0]
    const tagValue = obj[tag]
    if (tag === BLOB_TAG && typeof tagValue === 'string') {
      return Buffer.from(tagValue, 'hex')
    }
    if (tag === INT_TAG && typeof tagValue === 'string') {
      return BigInt(tagValue)
    }
  }
  const out: Record<string, unknown> = {}
  for (const k of keys) {
    out[k] = decodeTaggedValues(obj[k])
  }
  return out
}
