const HEX = '0123456789abcdef'

export function rowidKey(value: unknown): string {
  return `rowid:${String(value)}`
}

export function encodeRowKey(values: readonly unknown[]): string {
  let key = ''
  for (const value of values) {
    const part = encodePart(value)
    key += `${part.length}:${part}`
  }
  return key
}

function encodePart(value: unknown): string {
  if (value === null || value === undefined) return 'z'
  if (typeof value === 'bigint') return `n${value.toString()}`
  if (typeof value === 'number') return `n${Number.isInteger(value) ? value.toFixed(0) : value.toString()}`
  if (typeof value === 'boolean') return `n${value ? '1' : '0'}`
  if (typeof value === 'string') return `s${value}`
  if (value instanceof Uint8Array) return `b${toHex(value)}`
  if (ArrayBuffer.isView(value)) {
    const view = value as ArrayBufferView
    return `b${toHex(new Uint8Array(view.buffer, view.byteOffset, view.byteLength))}`
  }
  return `s${String(value)}`
}

function toHex(bytes: Uint8Array): string {
  let hex = ''
  for (const byte of bytes) {
    hex += HEX[byte >> 4] + HEX[byte & 15]
  }
  return hex
}
