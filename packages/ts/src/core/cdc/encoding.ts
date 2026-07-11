export const BLOB_TAG = '__sirannon_blob'
export const INT_TAG = '__sirannon_int'

export const SAFE_INT_BOUND_TEXT = '9007199254740991'
const SAFE_INT_BOUND = 9007199254740991n
const HEX_BYTES_RE = /^[0-9a-fA-F]*$/
/**
 * SQLite integers span at most 19 decimal digits (int64). Rejecting longer
 * payloads before BigInt parses them keeps a hostile client from submitting
 * megabyte-long digit strings that cost quadratic parse time.
 */
const INT_PAYLOAD_RE = /^-?\d{1,19}$/

/**
 * Re-tags native values a decoded change event carries (`BigInt`, binary) into
 * the same JSON envelope the CDC trigger writes, so a change frame stays
 * JSON-serialisable on the wire and the client decodes it back with
 * {@link decodeTaggedValues}. The inverse of {@link decodeTaggedValues}.
 *
 * Also the single normalisation pass for server query rows: a `BigInt` inside
 * the JS safe range narrows to a plain number, matching the spec that only
 * integers beyond 2^53 - 1 cross the wire tagged, so raw driver rows can be
 * encoded directly without a preceding narrowing walk.
 */
export function encodeTaggedValues(value: unknown): unknown {
  if (typeof value === 'bigint') {
    if (value >= -SAFE_INT_BOUND && value <= SAFE_INT_BOUND) {
      return Number(value)
    }
    return { [INT_TAG]: value.toString() }
  }
  if (isBinaryValue(value)) {
    return { [BLOB_TAG]: encodeHexBytes(value) }
  }
  if (value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) {
    return value.map(encodeTaggedValues)
  }
  // Objects with custom JSON behaviour (e.g. Date) keep it; walking them as
  // plain records would erase the value JSON.stringify produces. Binary values
  // are enveloped above, so Buffer's own toJSON never reaches this check.
  if (typeof (value as { toJSON?: unknown }).toJSON === 'function') {
    return value
  }
  const obj = value as Record<string, unknown>
  const out: Record<string, unknown> = {}
  for (const k of Object.keys(obj)) {
    out[k] = encodeTaggedValues(obj[k])
  }
  return out
}

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
      if (!INT_PAYLOAD_RE.test(tagValue)) {
        throw new Error('Invalid tagged integer payload')
      }
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

function encodeHexBytes(bytes: Uint8Array | Buffer): string {
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes).toString('hex').toUpperCase()
  }
  let hex = ''
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, '0')
  }
  return hex.toUpperCase()
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
