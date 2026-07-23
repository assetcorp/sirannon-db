export function canonicaliseForChecksum(value: unknown): string {
  return canonicaliseValue(value)
}

function canonicaliseValue(v: unknown): string {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (typeof v === 'string') return JSON.stringify(v)
  if (typeof v === 'bigint') return `{"__sint":"${v.toString()}"}`
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return `{"__sint":"${v.toString()}"}`
    if (Number.isFinite(v)) return JSON.stringify(v)
    return 'null'
  }
  if (typeof v === 'object') {
    if (v instanceof Uint8Array) {
      const buf = Buffer.isBuffer(v) ? (v as Buffer) : Buffer.from(v)
      return `{"__blob":"${buf.toString('base64')}"}`
    }
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(v)) {
      return `{"__blob":"${(v as Buffer).toString('base64')}"}`
    }
    if (Array.isArray(v)) {
      return `[${v.map(canonicaliseValue).join(',')}]`
    }
    const obj = v as Record<string, unknown>
    const parts: string[] = []
    for (const key of Object.keys(obj).sort()) {
      const val = obj[key]
      if (val === undefined) continue
      parts.push(`${JSON.stringify(key)}:${canonicaliseValue(val)}`)
    }
    return `{${parts.join(',')}}`
  }
  return 'null'
}
