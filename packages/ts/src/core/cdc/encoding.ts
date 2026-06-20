export const BLOB_TAG = '__sirannon_blob'
export const INT_TAG = '__sirannon_int'

export const SAFE_INT_BOUND_TEXT = '9007199254740991'
const HEX_BYTES_RE = /^[0-9a-fA-F]*$/

export function decodeTaggedValues(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value
  if (isBinaryValue(value)) return value
  if (Array.isArray(value)) {
    return value.map(decodeTaggedValues)
  }
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1) {
    const tag = keys[0]
    const tagValue = obj[tag]
    if (tag === BLOB_TAG && typeof tagValue === 'string') {
      return decodeHexBytes(tagValue)
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

function isBinaryValue(value: unknown): value is Uint8Array | Buffer {
  return value instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(value))
}

function decodeHexBytes(hex: string): Uint8Array | Buffer {
  if (hex.length % 2 !== 0 || !HEX_BYTES_RE.test(hex)) {
    throw new Error('Invalid CDC BLOB hex payload')
  }

  if (typeof Buffer !== 'undefined') {
    return Buffer.from(hex, 'hex')
  }

  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < hex.length; i += 2) {
    const byte = Number.parseInt(hex.slice(i, i + 2), 16)
    if (Number.isNaN(byte)) {
      throw new Error('Invalid CDC BLOB hex payload')
    }
    bytes[i / 2] = byte
  }
  return bytes
}
